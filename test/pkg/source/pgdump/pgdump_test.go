package pgdump_test

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"github.com/mittwald/brudi/pkg/source"
	"io/ioutil"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/docker/go-connections/nat"
	_ "github.com/jackc/pgx/stdlib"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"gotest.tools/assert"
)

const pgPort = "5432"

type PGDumpTestSuite struct {
	suite.Suite
}

type TestStruct struct {
	ID   int
	Name string
}

var pgRequest = testcontainers.ContainerRequest{
	Image:        "postgres:12",
	ExposedPorts: []string{fmt.Sprintf("%s/tcp", pgPort)},
	Env: map[string]string{
		"POSTGRES_PASSWORD": "postgresroot",
		"POSTGRES_USER":     "postgresuser",
		"POSTGRES_DB":       "postgres",
	},
	WaitingFor: wait.ForLog("database system is ready to accept connections"),
}

var resticReq = testcontainers.ContainerRequest{
	Image:        "restic/rest-server:latest",
	ExposedPorts: []string{"8000/tcp"},
	Env: map[string]string{
		"OPTIONS":         "--no-auth",
		"RESTIC_PASSWORD": "mongorepo",
	},
	VolumeMounts: map[string]string{
		"mysql-data": "/var/lib/mysql",
	},
}

type TestContainerSetup struct {
	Container testcontainers.Container
	Address   string
	Port      string
}

func (mySQLDumpTestSuite *PGDumpTestSuite) SetupTest() {
	viper.Reset()
	viper.SetConfigType("yaml")
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.AutomaticEnv()
}

func (mySQLDumpTestSuite *PGDumpTestSuite) TearDownTest() {
	viper.Reset()
}

func newTestContainerSetup(ctx context.Context, request *testcontainers.ContainerRequest, port nat.Port) (TestContainerSetup, error) {
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

// createMongoConfig creates a brudi config for the mongodump command
func createPGConfig(container TestContainerSetup, useRestic bool, resticIP, resticPort string) []byte {
	fmt.Println(resticIP)
	fmt.Println(resticPort)
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
      file: /tmp/postgres.dump
    additionalArgs: []
`, "127.0.0.1", container.Port))
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
      file: /tmp/postgres.dump
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
`, "127.0.0.1", container.Port, resticIP, resticPort))
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

func restorePGFromBackup(filename string, database *sql.DB) error {
	file, err := ioutil.ReadFile(filename)
	if err != nil {
		return err
	}

	requests := strings.Split(string(file), ";\n")

	for _, request := range requests {
		fmt.Println(request)
		_, err := database.Exec(request)
		if err != nil {
			return err
		}
	}
	return nil
}

func (pgDumpTestSuite *PGDumpTestSuite) TestBasicPGDump() {
	ctx := context.Background()
	port, err := nat.NewPort("tcp", fmt.Sprintf("%s", pgPort))
	pgDumpTestSuite.Require().NoError(err)

	// create a mysql container to test backup function
	pgBackupTarget, err := newTestContainerSetup(ctx, &pgRequest, port)
	pgDumpTestSuite.Require().NoError(err)
	fmt.Println(pgBackupTarget.Port)
	// connect to mysql database using the driver
	connectionString := fmt.Sprintf("user=postgresuser password=postgresroot host=%s port=%s database=%s sslmode=disable",
		pgBackupTarget.Address, pgBackupTarget.Port, "postgres")
	fmt.Println(connectionString)
	db, err := sql.Open("pgx", connectionString)
	pgDumpTestSuite.Require().NoError(err)
	time.Sleep(1 * time.Second)
	err = db.Ping()
	pgDumpTestSuite.Require().NoError(err)

	_, err = db.Exec("SELECT * FROM pg_catalog.pg_tables;")
	pgDumpTestSuite.Require().NoError(err)

	// Create test table
	out, err := db.Exec("CREATE TABLE test(id serial PRIMARY KEY, name VARCHAR(100) NOT NULL)")
	fmt.Println(out)
	pgDumpTestSuite.Require().NoError(err)

	// create test data and write it to database
	testData, err := prepareTestData(db)
	pgDumpTestSuite.Require().NoError(err)

	err = db.Close()
	pgDumpTestSuite.Require().NoError(err)

	testMySQLConfig := createPGConfig(pgBackupTarget, false, "", "")
	err = viper.ReadConfig(bytes.NewBuffer(testMySQLConfig))
	pgDumpTestSuite.Require().NoError(err)

	// perform backup action on first mysql container
	err = source.DoBackupForKind(ctx, "pgdump", false, false, false)
	pgDumpTestSuite.Require().NoError(err)

	err = pgBackupTarget.Container.Terminate(ctx)
	pgDumpTestSuite.Require().NoError(err)

	// setup second mysql container to test if correct data is restored
	pgRestoreTarget, err := newTestContainerSetup(ctx, &pgRequest, port)
	pgDumpTestSuite.Require().NoError(err)

	connectionString2 := fmt.Sprintf("user=postgresuser password=postgresroot host=%s port=%s database=%s sslmode=disable",
		pgRestoreTarget.Address, pgRestoreTarget.Port, "postgres")
	dbRestore, err := sql.Open("pgx", connectionString2)
	pgDumpTestSuite.Require().NoError(err)

	time.Sleep(1 * time.Second)
	err = dbRestore.Ping()
	pgDumpTestSuite.Require().NoError(err)

	fmt.Println("select pre restore")
	_, err = dbRestore.Exec("SELECT * FROM pg_catalog.pg_tables;")
	pgDumpTestSuite.Require().NoError(err)

	fmt.Println("execute restore")
	// restore server from mysqldump
	err = restorePGFromBackup("/tmp/postgres.dump", dbRestore)
	pgDumpTestSuite.Require().NoError(err)
	fmt.Println("finished restore")

	err = os.Remove("/tmp/postgres.dump")
	pgDumpTestSuite.Require().NoError(err)

	// check if data was restored correctly
	result, err := dbRestore.Query("SELECT * FROM test")
	pgDumpTestSuite.Require().NoError(err)
	pgDumpTestSuite.Require().NoError(result.Err())
	defer result.Close()

	var restoreResult []TestStruct
	for result.Next() {
		var test TestStruct
		err := result.Scan(&test.ID, &test.Name)
		pgDumpTestSuite.Require().NoError(err)
		restoreResult = append(restoreResult, test)
	}

	assert.DeepEqual(pgDumpTestSuite.T(), testData, restoreResult)
}

func TestMySQLDumpTestSuite(t *testing.T) {
	suite.Run(t, new(PGDumpTestSuite))
}
