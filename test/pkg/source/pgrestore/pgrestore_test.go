package pgrestore_test

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"github.com/docker/go-connections/nat"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/jackc/pgx/stdlib"
	"github.com/mittwald/brudi/pkg/source"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"gotest.tools/assert"
)

const pgPort = "5432/tcp"
const ResticPort = "8000/tcp"
const backupPath = "/tmp/postgres.dump.tar"

type PGRestoreTestSuite struct {
	suite.Suite
}

type TestStruct struct {
	ID   int
	Name string
}

type TestContainerSetup struct {
	Container testcontainers.Container
	Address   string
	Port      string
}

var ResticReq = testcontainers.ContainerRequest{
	Image:        "restic/rest-server:latest",
	ExposedPorts: []string{ResticPort},
	Env: map[string]string{
		"OPTIONS":         "--no-auth",
		"RESTIC_PASSWORD": "mongorepo",
	},
}

func NewTestContainerSetup(ctx context.Context, request *testcontainers.ContainerRequest, port nat.Port) (TestContainerSetup, error) {
	result := TestContainerSetup{}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: *request,
		Started:          true,
	})
	if err != nil {
		return TestContainerSetup{}, err
	}
	result.Container = container
	contPort, err := container.MappedPort(ctx, port)
	if err != nil {
		return TestContainerSetup{}, err
	}
	result.Port = fmt.Sprint(contPort.Int())
	host, err := container.Host(ctx)
	if err != nil {
		return TestContainerSetup{}, err
	}
	result.Address = host

	return result, nil
}

var pgRequest = testcontainers.ContainerRequest{
	Image:        "postgres:12",
	ExposedPorts: []string{pgPort},
	Env: map[string]string{
		"POSTGRES_PASSWORD": "postgresroot",
		"POSTGRES_USER":     "postgresuser",
		"POSTGRES_DB":       "postgres",
	},
	WaitingFor: wait.ForLog("database system is ready to accept connections"),
}

func (pgRestoreTestSuite *PGRestoreTestSuite) SetupTest() {
	viper.Reset()
	viper.SetConfigType("yaml")
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.AutomaticEnv()
}

func (pgRestoreTestSuite *PGRestoreTestSuite) TearDownTest() {
	viper.Reset()
}

func createPSQLRestoreConfig(container TestContainerSetup, resticIP, resticPort string) []byte {
	return []byte(fmt.Sprintf(`
psql:
  options:
    flags:
      host: %s
      port: %s
      password: postgresroot
      user: postgresuser
      dbName: postgres
    additionalArgs: []
    sourceFile: %s
restic:
  global:
    flags:
      repo: rest:http://%s:%s/
  restore:
    flags:
      target: "/"
    id: "latest"
`, "127.0.0.1", container.Port, backupPath, resticIP, resticPort))
}

func createPGRestoreConfig(container TestContainerSetup, resticIP, resticPort string) []byte {
	return []byte(fmt.Sprintf(`
pgrestore:
  options:
    flags:
      host: %s
      port: %s
      password: postgresroot
      username: postgresuser
      dbName: postgres
    additionalArgs: []
    sourceFile: %s
restic:
  global:
    flags:
      repo: rest:http://%s:%s/
  restore:
    flags:
      target: "/"
    id: "latest"
`, "127.0.0.1", container.Port, backupPath, resticIP, resticPort))
}

// createMongoConfig creates a brudi config for the pgdump command
func createPGConfig(container TestContainerSetup, useRestic bool, format, resticIP, resticPort string) []byte {
	if !useRestic {
		return []byte(fmt.Sprintf(`
pgdump:
  options:
    flags:
      host: %s
      port: %s
      password: postgresroot
      username: postgresuser
      dbName: postgres
      file: %s
      format: %s
    additionalArgs: []
`, "127.0.0.1", container.Port, backupPath, format))
	}
	return []byte(fmt.Sprintf(`
pgdump:
  options:
    flags:
      host: %s
      port: %s
      password: postgresroot
      username: postgresuser
      dbName: postgres
      format: %s
      file: %s
    additionalArgs: []
restic:
  global:
    flags:
      repo: rest:http://%s:%s/
  forget:
    flags:
      keepLast: 1
      keepHourly: 0
      keepDaily: 0
      keepWeekly: 0
      keepMonthly: 0
      keepYearly: 0
`, "127.0.0.1", container.Port, format, backupPath, resticIP, resticPort))
}

func prepareTestData(database *sql.DB) ([]TestStruct, error) {
	var err error
	testStruct1 := TestStruct{2, "TEST"}
	testData := []TestStruct{testStruct1}
	var insert *sql.Rows
	for idx := range testData {
		insert, err = database.Query(fmt.Sprintf("INSERT INTO test (id, name) VALUES ( %d, '%s' )", testData[idx].ID, testData[idx].Name))
		if err != nil {
			return []TestStruct{}, err
		}
		if insert.Err() != nil {
			return []TestStruct{}, insert.Err()
		}
	}
	err = insert.Close()
	if err != nil {
		return []TestStruct{}, err
	}
	return testData, nil
}

func scanResult(result *sql.Rows) ([]TestStruct, error) {
	var restoreResult []TestStruct
	for result.Next() {
		var test TestStruct
		err := result.Scan(&test.ID, &test.Name)
		if err != nil {
			return []TestStruct{}, err
		}
		restoreResult = append(restoreResult, test)
	}
	return restoreResult, nil
}

func pgRestoreHelper(pgRestoreTestSuite *PGRestoreTestSuite, useRestic bool, resticContainer TestContainerSetup) {
	ctx := context.Background()

	// create a mysql container to test backup function
	pgBackupTarget, err := NewTestContainerSetup(ctx, &pgRequest, pgPort)
	pgRestoreTestSuite.Require().NoError(err)

	// connect to mysql database using the driver
	connectionString := fmt.Sprintf("user=postgresuser password=postgresroot host=%s port=%s database=%s sslmode=disable",
		pgBackupTarget.Address, pgBackupTarget.Port, "postgres")
	db, err := sql.Open("pgx", connectionString)
	pgRestoreTestSuite.Require().NoError(err)

	// these are necessary, otherwise pgserver resets connections
	time.Sleep(1 * time.Second)
	err = db.Ping()
	pgRestoreTestSuite.Require().NoError(err)

	// Create test table
	_, err = db.Exec("CREATE TABLE test(id serial PRIMARY KEY, name VARCHAR(100) NOT NULL)")
	pgRestoreTestSuite.Require().NoError(err)

	// create test data and write it to database
	testData, err := prepareTestData(db)
	pgRestoreTestSuite.Require().NoError(err)

	err = db.Close()
	pgRestoreTestSuite.Require().NoError(err)

	testPGConfig := createPGConfig(pgBackupTarget, true, "tar", resticContainer.Address, resticContainer.Port)
	err = viper.ReadConfig(bytes.NewBuffer(testPGConfig))
	pgRestoreTestSuite.Require().NoError(err)

	// perform backup action on first pgsql container
	err = source.DoBackupForKind(ctx, "pgdump", false, useRestic, false)
	pgRestoreTestSuite.Require().NoError(err)

	err = pgBackupTarget.Container.Terminate(ctx)
	pgRestoreTestSuite.Require().NoError(err)

	// setup second pgsql container to test if correct data is restored
	pgRestoreTarget, err := NewTestContainerSetup(ctx, &pgRequest, pgPort)
	pgRestoreTestSuite.Require().NoError(err)

	connectionString2 := fmt.Sprintf("user=postgresuser password=postgresroot host=%s port=%s database=%s sslmode=disable",
		pgRestoreTarget.Address, pgRestoreTarget.Port, "postgres")
	dbRestore, err := sql.Open("pgx", connectionString2)
	pgRestoreTestSuite.Require().NoError(err)

	time.Sleep(1 * time.Second)
	err = dbRestore.Ping()
	pgRestoreTestSuite.Require().NoError(err)

	testPGRestoreConfig := createPGRestoreConfig(pgRestoreTarget, resticContainer.Address, resticContainer.Port)
	err = viper.ReadConfig(bytes.NewBuffer(testPGRestoreConfig))
	pgRestoreTestSuite.Require().NoError(err)

	err = source.DoRestoreForKind(ctx, "pgrestore", false, useRestic, false)
	pgRestoreTestSuite.Require().NoError(err)

	err = os.Remove(backupPath)
	pgRestoreTestSuite.Require().NoError(err)

	// check if data was restored correctly
	result, err := dbRestore.Query("SELECT * FROM test")
	pgRestoreTestSuite.Require().NoError(err)
	pgRestoreTestSuite.Require().NoError(result.Err())
	defer result.Close()

	restoreResult, err := scanResult(result)
	pgRestoreTestSuite.Require().NoError(err)

	assert.DeepEqual(pgRestoreTestSuite.T(), testData, restoreResult)

	err = pgRestoreTarget.Container.Terminate(ctx)
	pgRestoreTestSuite.Require().NoError(err)
	err = dbRestore.Close()
	pgRestoreTestSuite.Require().NoError(err)
}

func (pgRestoreTestSuite *PGRestoreTestSuite) TestBasicPGRestore() {
	pgRestoreHelper(pgRestoreTestSuite, false, TestContainerSetup{Port: "", Address: ""})
}

func (pgRestoreTestSuite *PGRestoreTestSuite) TestPGRestoreRestic() {
	ctx := context.Background()
	resticContainer, err := NewTestContainerSetup(ctx, &ResticReq, ResticPort)
	pgRestoreTestSuite.Require().NoError(err)

	pgRestoreHelper(pgRestoreTestSuite, true, resticContainer)
}

func psqlRestoreHelper(pgRestoreTestSuite *PGRestoreTestSuite, useRestic bool, resticContainer TestContainerSetup) {
	ctx := context.Background()

	// create a mysql container to test backup function
	pgBackupTarget, err := NewTestContainerSetup(ctx, &pgRequest, pgPort)
	pgRestoreTestSuite.Require().NoError(err)

	// connect to mysql database using the driver
	connectionString := fmt.Sprintf("user=postgresuser password=postgresroot host=%s port=%s database=%s sslmode=disable",
		pgBackupTarget.Address, pgBackupTarget.Port, "postgres")
	db, err := sql.Open("pgx", connectionString)
	pgRestoreTestSuite.Require().NoError(err)

	// these are necessary, otherwise pgserver resets connections
	time.Sleep(1 * time.Second)
	err = db.Ping()
	pgRestoreTestSuite.Require().NoError(err)

	// Create test table
	_, err = db.Exec("CREATE TABLE test(id serial PRIMARY KEY, name VARCHAR(100) NOT NULL)")
	pgRestoreTestSuite.Require().NoError(err)

	// create test data and write it to database
	testData, err := prepareTestData(db)
	pgRestoreTestSuite.Require().NoError(err)

	err = db.Close()
	pgRestoreTestSuite.Require().NoError(err)

	testPGConfig := createPGConfig(pgBackupTarget, true, "plain", resticContainer.Address, resticContainer.Port)
	err = viper.ReadConfig(bytes.NewBuffer(testPGConfig))
	pgRestoreTestSuite.Require().NoError(err)

	// perform backup action on first pgsql container
	err = source.DoBackupForKind(ctx, "pgdump", false, useRestic, false)
	pgRestoreTestSuite.Require().NoError(err)

	err = pgBackupTarget.Container.Terminate(ctx)
	pgRestoreTestSuite.Require().NoError(err)

	// setup second pgsql container to test if correct data is restored
	pgRestoreTarget, err := NewTestContainerSetup(ctx, &pgRequest, pgPort)
	pgRestoreTestSuite.Require().NoError(err)

	connectionString2 := fmt.Sprintf("user=postgresuser password=postgresroot host=%s port=%s database=%s sslmode=disable",
		pgRestoreTarget.Address, pgRestoreTarget.Port, "postgres")
	dbRestore, err := sql.Open("pgx", connectionString2)
	pgRestoreTestSuite.Require().NoError(err)

	time.Sleep(1 * time.Second)
	err = dbRestore.Ping()
	pgRestoreTestSuite.Require().NoError(err)

	testPSQLRestoreConfig := createPSQLRestoreConfig(pgRestoreTarget, resticContainer.Address, resticContainer.Port)
	err = viper.ReadConfig(bytes.NewBuffer(testPSQLRestoreConfig))
	pgRestoreTestSuite.Require().NoError(err)

	err = source.DoRestoreForKind(ctx, "psql", false, useRestic, false)
	pgRestoreTestSuite.Require().NoError(err)

	err = os.Remove(backupPath)
	pgRestoreTestSuite.Require().NoError(err)

	// check if data was restored correctly
	result, err := dbRestore.Query("SELECT * FROM test")
	pgRestoreTestSuite.Require().NoError(err)
	pgRestoreTestSuite.Require().NoError(result.Err())
	defer result.Close()

	restoreResult, err := scanResult(result)
	pgRestoreTestSuite.Require().NoError(err)

	assert.DeepEqual(pgRestoreTestSuite.T(), testData, restoreResult)

	err = pgRestoreTarget.Container.Terminate(ctx)
	pgRestoreTestSuite.Require().NoError(err)
	err = dbRestore.Close()
	pgRestoreTestSuite.Require().NoError(err)
}

func (pgRestoreTestSuite *PGRestoreTestSuite) TestBasicPSQLRestore() {
	psqlRestoreHelper(pgRestoreTestSuite, false, TestContainerSetup{Port: "", Address: ""})
}

func (pgRestoreTestSuite *PGRestoreTestSuite) TestPSQLRestoreRestic() {
	ctx := context.Background()
	resticContainer, err := NewTestContainerSetup(ctx, &ResticReq, ResticPort)
	pgRestoreTestSuite.Require().NoError(err)

	psqlRestoreHelper(pgRestoreTestSuite, true, resticContainer)
}

func TestPGRestoreTestSuite(t *testing.T) {
	suite.Run(t, new(PGRestoreTestSuite))
}
