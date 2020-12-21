package sqldump_test

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
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

type MySQLDumpTestSuite struct {
	suite.Suite
}

type TestStruct struct {
	ID   int
	Name string
}

var mySQLRequest = testcontainers.ContainerRequest{
	Image:        "mysql:8",
	ExposedPorts: []string{"3306/tcp"},
	Env: map[string]string{
		"MYSQL_ROOT_PASSWORD": "mysqlroot",
		"MYSQL_DATABASE":      "mysql",
		"MYSQL_USER":          "mysqluser",
		"MYSQL_PASSWORD":      "mysql",
	},
	Cmd:        []string{"--default-authentication-plugin=mysql_native_password"},
	WaitingFor: wait.ForLog("port: 3306  MySQL Community Server - GPL"),
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

func (mySQLDumpTestSuite *MySQLDumpTestSuite) SetupTest() {
	viper.Reset()
	viper.SetConfigType("yaml")
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.AutomaticEnv()
}

func (mySQLDumpTestSuite *MySQLDumpTestSuite) TearDownTest() {
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
func createMySQLConfig(container TestContainerSetup, useRestic bool, resticIP, resticPort string) []byte {
	fmt.Println(resticIP)
	fmt.Println(resticPort)
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
      resultFile: /tmp/test.sqldump
    additionalArgs: []
`, "127.0.0.1", container.Port))
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
      resultFile: /tmp/test.sqldump
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

func restoreSQLFromBackup(filename string, database *sql.DB) error {
	file, err := ioutil.ReadFile(filename)
	if err != nil {
		return err
	}

	requests := strings.Split(string(file), ";\n")

	for _, request := range requests {
		_, err := database.Exec(request)
		if err != nil {
			return err
		}
	}
	return nil
}

func (mySQLDumpTestSuite *MySQLDumpTestSuite) TestBasicMySQLDump() {
	ctx := context.Background()

	// create a mysql container to test backup function
	mySQLBackupTarget, err := newTestContainerSetup(ctx, &mySQLRequest, "3306/tcp")
	mySQLDumpTestSuite.Require().NoError(err)

	// connect to mysql database using the driver
	connectionString := fmt.Sprintf("root:mysqlroot@tcp(%s:%s)/%s?tls=skip-verify",
		mySQLBackupTarget.Address, mySQLBackupTarget.Port, "mysql")
	db, err := sql.Open("mysql", connectionString)
	mySQLDumpTestSuite.Require().NoError(err)

	// Create test table
	_, err = db.Exec("CREATE TABLE test(id INT NOT NULL AUTO_INCREMENT, name VARCHAR(100) NOT NULL, PRIMARY KEY ( id ));")
	mySQLDumpTestSuite.Require().NoError(err)

	// create test data and write it to database
	testData, err := prepareTestData(db)
	mySQLDumpTestSuite.Require().NoError(err)

	err = db.Close()
	mySQLDumpTestSuite.Require().NoError(err)

	testMySQLConfig := createMySQLConfig(mySQLBackupTarget, false, "", "")
	err = viper.ReadConfig(bytes.NewBuffer(testMySQLConfig))
	mySQLDumpTestSuite.Require().NoError(err)

	// perform backup action on first mysql container
	err = source.DoBackupForKind(ctx, "mysqldump", false, false, false)
	mySQLDumpTestSuite.Require().NoError(err)

	err = mySQLBackupTarget.Container.Terminate(ctx)
	mySQLDumpTestSuite.Require().NoError(err)

	// setup second mysql container to test if correct data is restored
	mySQLRestoreTarget, err := newTestContainerSetup(ctx, &mySQLRequest, "3306/tcp")
	mySQLDumpTestSuite.Require().NoError(err)

	connectionString2 := fmt.Sprintf("root:mysqlroot@tcp(%s:%s)/%s?tls=skip-verify",
		mySQLRestoreTarget.Address, mySQLRestoreTarget.Port, "mysql")
	dbRestore, err := sql.Open("mysql", connectionString2)
	mySQLDumpTestSuite.Require().NoError(err)

	// restore server from mysqldump
	err = restoreSQLFromBackup("/tmp/test.sqldump", dbRestore)
	mySQLDumpTestSuite.Require().NoError(err)

	err = os.Remove("/tmp/test.sqldump")
	mySQLDumpTestSuite.Require().NoError(err)

	// check if data was restored correctly
	result, err := dbRestore.Query("SELECT * FROM test")
	mySQLDumpTestSuite.Require().NoError(err)
	mySQLDumpTestSuite.Require().NoError(result.Err())
	defer result.Close()

	var restoreResult []TestStruct
	for result.Next() {
		var test TestStruct
		err := result.Scan(&test.ID, &test.Name)
		mySQLDumpTestSuite.Require().NoError(err)
		restoreResult = append(restoreResult, test)
	}

	assert.DeepEqual(mySQLDumpTestSuite.T(), testData, restoreResult)
}

func (mySQLDumpTestSuite *MySQLDumpTestSuite) TestBasicMySQLDumpRestic() {
	ctx := context.Background()

	mySQLBackupTarget, err := newTestContainerSetup(ctx, &mySQLRequest, "3306/tcp")
	mySQLDumpTestSuite.Require().NoError(err)

	// setup a container running the restic rest-server
	resticContainer, err := newTestContainerSetup(ctx, &resticReq, "8000/tcp")
	mySQLDumpTestSuite.Require().NoError(err)

	connectionString := fmt.Sprintf("root:mysqlroot@tcp(%s:%s)/%s?tls=skip-verify",
		mySQLBackupTarget.Address, mySQLBackupTarget.Port, "mysql")
	db, err := sql.Open("mysql", connectionString)
	mySQLDumpTestSuite.Require().NoError(err)

	_, err = db.Exec("CREATE TABLE test(id INT NOT NULL AUTO_INCREMENT, name VARCHAR(100) NOT NULL, PRIMARY KEY ( id ));")
	mySQLDumpTestSuite.Require().NoError(err)

	testData, err := prepareTestData(db)
	mySQLDumpTestSuite.Require().NoError(err)

	err = db.Close()
	mySQLDumpTestSuite.Require().NoError(err)

	testMySQLConfig := createMySQLConfig(mySQLBackupTarget, true, resticContainer.Address, resticContainer.Port)
	err = viper.ReadConfig(bytes.NewBuffer(testMySQLConfig))
	mySQLDumpTestSuite.Require().NoError(err)

	err = source.DoBackupForKind(ctx, "mysqldump", false, true, false)
	mySQLDumpTestSuite.Require().NoError(err)

	err = mySQLBackupTarget.Container.Terminate(ctx)
	mySQLDumpTestSuite.Require().NoError(err)

	mySQLRestoreTarget, err := newTestContainerSetup(ctx, &mySQLRequest, "3306/tcp")
	mySQLDumpTestSuite.Require().NoError(err)

	connectionString2 := fmt.Sprintf("root:mysqlroot@tcp(%s:%s)/%s?tls=skip-verify",
		mySQLRestoreTarget.Address, mySQLRestoreTarget.Port, "mysql")
	dbRestore, err := sql.Open("mysql", connectionString2)
	mySQLDumpTestSuite.Require().NoError(err)

	// restore backup file from restic repository
	cmd := exec.CommandContext(ctx, "restic", "restore", "-r", fmt.Sprintf("rest:http://%s:%s/",
		resticContainer.Address, resticContainer.Port),
		"--target", "data", "latest")
	_, err = cmd.CombinedOutput()
	mySQLDumpTestSuite.Require().NoError(err)

	err = restoreSQLFromBackup("data/tmp/test.sqldump", dbRestore)
	mySQLDumpTestSuite.Require().NoError(err)

	// delete folder with backup file
	err = os.RemoveAll("data")
	mySQLDumpTestSuite.Require().NoError(err)

	result, err := dbRestore.Query("SELECT * FROM test")
	mySQLDumpTestSuite.Require().NoError(err)
	mySQLDumpTestSuite.Require().NoError(result.Err())
	defer result.Close()

	var restoreResult []TestStruct
	for result.Next() {
		var test TestStruct
		err := result.Scan(&test.ID, &test.Name)
		mySQLDumpTestSuite.Require().NoError(err)
		restoreResult = append(restoreResult, test)
	}

	assert.DeepEqual(mySQLDumpTestSuite.T(), testData, restoreResult)
}

func TestMySQLDumpTestSuite(t *testing.T) {
	suite.Run(t, new(MySQLDumpTestSuite))
}
