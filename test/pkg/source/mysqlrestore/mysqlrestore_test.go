package sqlrestore_test

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/mittwald/brudi/pkg/source"

	"github.com/docker/go-connections/nat"
	_ "github.com/go-sql-driver/mysql"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"gotest.tools/assert"
)

const sqlPort = "3306/tcp"
const backupPath = "/tmp/test.sqldump"
const ResticPort = "8000/tcp"

type MySQLRestoreTestSuite struct {
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

var mySQLRequest = testcontainers.ContainerRequest{
	Image:        "mysql:8",
	ExposedPorts: []string{sqlPort},
	Env: map[string]string{
		"MYSQL_ROOT_PASSWORD": "mysqlroot",
		"MYSQL_DATABASE":      "mysql",
		"MYSQL_USER":          "mysqluser",
		"MYSQL_PASSWORD":      "mysql",
	},
	Cmd:        []string{"--default-authentication-plugin=mysql_native_password"},
	WaitingFor: wait.ForLog("port: 3306  MySQL Community Server - GPL"),
}

func createMySQLConfig(container TestContainerSetup, useRestic bool, resticIP, resticPort string) []byte {
	if !useRestic {
		return []byte(fmt.Sprintf(`
mysqldump:
  options:
    flags:
      host: %s
      port: %s
      password: mysqlroot
      user: root
      opt: true
      allDatabases: true
      resultFile: %s
    additionalArgs: []
`, "127.0.0.1", container.Port, backupPath)) // address is hardcoded because the sql driver doesn't like 'localhost'
	}
	return []byte(fmt.Sprintf(`
mysqldump:
  options:
    flags:
      host: %s
      port: %s
      password: mysqlroot
      user: root
      opt: true
      allDatabases: true
      resultFile: %s
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
`, "127.0.0.1", container.Port, backupPath, resticIP, resticPort))
}

func prepareTestData(database *sql.DB) ([]TestStruct, error) {
	var err error
	testStruct1 := TestStruct{2, "TEST"}
	testData := []TestStruct{testStruct1}
	var insert *sql.Rows
	for idx := range testData {
		insert, err = database.Query(fmt.Sprintf("INSERT INTO test VALUES ( %d, '%s' )", testData[idx].ID, testData[idx].Name))
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

func (mySQLRestoreTestSuite *MySQLRestoreTestSuite) SetupTest() {
	viper.Reset()
	viper.SetConfigType("yaml")
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.AutomaticEnv()
}

func (mySQLRestoreTestSuite *MySQLRestoreTestSuite) TearDownTest() {
	viper.Reset()
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

// createMySQLRestoreConfig creates a brudi config for the sqlrestore command
func createMySQLRestoreConfig(container TestContainerSetup, resticIP, resticPort string) []byte {
	return []byte(fmt.Sprintf(`
      mysqlrestore:
        options:
          flags:
            host: %s
            port: %s
            password: mysqlroot
            user: root
            opt: true
            database: mysql
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

func mySQLDoBackup(ctx context.Context, mySQLRestoreTestSuite *MySQLRestoreTestSuite, useRestic bool,
	resticContainer TestContainerSetup) []TestStruct {
	// create a mysql container to test backup function
	mySQLBackupTarget, err := NewTestContainerSetup(ctx, &mySQLRequest, sqlPort)
	mySQLRestoreTestSuite.Require().NoError(err)
	defer func() {
		err = mySQLBackupTarget.Container.Terminate(ctx)
		mySQLRestoreTestSuite.Require().NoError(err)
	}()

	// connect to mysql database using the driver
	connectionString := fmt.Sprintf("root:mysqlroot@tcp(%s:%s)/%s?tls=skip-verify",
	mySQLBackupTarget.Address, mySQLBackupTarget.Port, "mysql")
	db, err := sql.Open("mysql", connectionString)
	mySQLRestoreTestSuite.Require().NoError(err)
	defer func() {
		err = db.Close()
		mySQLRestoreTestSuite.Require().NoError(err)
	}()

	// Create test table
	_, err = db.Exec("CREATE TABLE test(id INT NOT NULL AUTO_INCREMENT, name VARCHAR(100) NOT NULL, PRIMARY KEY ( id ));")
	mySQLRestoreTestSuite.Require().NoError(err)

	// create test data and write it to database
	testData, err := prepareTestData(db)
	mySQLRestoreTestSuite.Require().NoError(err)

	testMySQLConfig := createMySQLConfig(mySQLBackupTarget, false, "", "")
	err = viper.ReadConfig(bytes.NewBuffer(testMySQLConfig))
	mySQLRestoreTestSuite.Require().NoError(err)

	// perform backup action on first mysql container
	err = source.DoBackupForKind(ctx, "mysqldump", false, false, false)
	mySQLRestoreTestSuite.Require().NoError(err)

	return testData
}

func (mySQLRestoreTestSuite *MySQLRestoreTestSuite) TestBasicMySQLRestore() {
	ctx := context.Background()

	testData := mySQLDoBackup(ctx, mySQLRestoreTestSuite, false, TestContainerSetup{Port: "", Address: ""})

	// setup second mysql container to test if correct data is restored
	mySQLRestoreTarget, err := NewTestContainerSetup(ctx, &mySQLRequest, sqlPort)
	mySQLRestoreTestSuite.Require().NoError(err)
	defer func() {
		err = mySQLRestoreTarget.Container.Terminate(ctx)
		mySQLRestoreTestSuite.Require().NoError(err)
	}()

	connectionString2 := fmt.Sprintf("root:mysqlroot@tcp(%s:%s)/%s?tls=skip-verify",
		mySQLRestoreTarget.Address, mySQLRestoreTarget.Port, "mysql")
	dbRestore, err := sql.Open("mysql", connectionString2)
	mySQLRestoreTestSuite.Require().NoError(err)
	defer func() {
		err = dbRestore.Close()
		mySQLRestoreTestSuite.Require().NoError(err)
	}()

	testMySQLRestoreConfig := createMySQLRestoreConfig(mySQLRestoreTarget, "", "")
	err = viper.ReadConfig(bytes.NewBuffer(testMySQLRestoreConfig))
	mySQLRestoreTestSuite.Require().NoError(err)

	// restore server from mysqldump
	err = source.DoRestoreForKind(ctx, "mysqlrestore", false, false, false)
	mySQLRestoreTestSuite.Require().NoError(err)

	err = os.Remove(backupPath)
	mySQLRestoreTestSuite.Require().NoError(err)

	// check if data was restored correctly
	result, err := dbRestore.Query("SELECT * FROM test")
	mySQLRestoreTestSuite.Require().NoError(err)
	mySQLRestoreTestSuite.Require().NoError(result.Err())
	defer result.Close()

	var restoreResult []TestStruct
	for result.Next() {
		var test TestStruct
		err := result.Scan(&test.ID, &test.Name)
		mySQLRestoreTestSuite.Require().NoError(err)
		restoreResult = append(restoreResult, test)
	}

	assert.DeepEqual(mySQLRestoreTestSuite.T(), testData, restoreResult)
}

func (mySQLRestoreTestSuite *MySQLRestoreTestSuite) TestMySQLRestoreRestic() {
	ctx := context.Background()

	resticContainer, err := NewTestContainerSetup(ctx, &ResticReq, ResticPort)
	mySQLRestoreTestSuite.Require().NoError(err)
	defer func() {
		err = resticContainer.Container.Terminate(ctx)
		mySQLRestoreTestSuite.Require().NoError(err)
	}()

    testData := mySQLDoBackup(ctx, mySQLRestoreTestSuite, true, resticContainer)

	// setup second mysql container to test if correct data is restored
	mySQLRestoreTarget, err := NewTestContainerSetup(ctx, &mySQLRequest, sqlPort)
	mySQLRestoreTestSuite.Require().NoError(err)
	defer func() {
		err = mySQLRestoreTarget.Container.Terminate(ctx)
		mySQLRestoreTestSuite.Require().NoError(err)
	}()

	connectionString2 := fmt.Sprintf("root:mysqlroot@tcp(%s:%s)/%s?tls=skip-verify",
		mySQLRestoreTarget.Address, mySQLRestoreTarget.Port, "mysql")
	dbRestore, err := sql.Open("mysql", connectionString2)
	mySQLRestoreTestSuite.Require().NoError(err)
	defer func() {
		err = dbRestore.Close()
		mySQLRestoreTestSuite.Require().NoError(err)
	}()

	testMySQLRestoreConfig := createMySQLRestoreConfig(mySQLRestoreTarget, resticContainer.Address, resticContainer.Port)
	err = viper.ReadConfig(bytes.NewBuffer(testMySQLRestoreConfig))
	mySQLRestoreTestSuite.Require().NoError(err)

	// restore server from mysqldump
	err = source.DoRestoreForKind(ctx, "mysqlrestore", false, true, false)
	mySQLRestoreTestSuite.Require().NoError(err)

	err = os.Remove(backupPath)
	mySQLRestoreTestSuite.Require().NoError(err)

	// check if data was restored correctly
	result, err := dbRestore.Query("SELECT * FROM test")
	mySQLRestoreTestSuite.Require().NoError(err)
	mySQLRestoreTestSuite.Require().NoError(result.Err())
	defer result.Close()

	var restoreResult []TestStruct
	for result.Next() {
		var test TestStruct
		err := result.Scan(&test.ID, &test.Name)
		mySQLRestoreTestSuite.Require().NoError(err)
		restoreResult = append(restoreResult, test)
	}

	assert.DeepEqual(mySQLRestoreTestSuite.T(), testData, restoreResult)
}

func TestMySQLRestoreTestSuite(t *testing.T) {
	suite.Run(t, new(MySQLRestoreTestSuite))
}
