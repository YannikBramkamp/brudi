package mongodump_test

import (
	"bytes"
	"context"
	"fmt"
	"github.com/mittwald/brudi/pkg/source"
	commons "github.com/mittwald/brudi/test/pkg/source/internal"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"gotest.tools/assert"
	"os"
	"os/exec"
	"testing"
)

const mongoPort = "27017/tcp"
const backupPath = "/tmp/dump.tar.gz"
const mongoPW = "mongodbroot"
const mongoUser = "root"
const dataDir = "data"
const dumpKind = "mongodump"
const restoreKind = "mongorestore"
const dbName = "test"
const collName = "testColl"
const mongoImage = "quay.io/bitnami/mongodb:latest"
const logString = "Waiting for connections"

// TestColl holds test data for integration tests
type TestColl struct {
	Name string
	Age  int
}

type MongoDumpTestSuite struct {
	suite.Suite
}

func (mongoDumpTestSuite *MongoDumpTestSuite) SetupTest() {
	commons.TestSetup()
}

// TearDownTest resets viper after a test
func (mongoDumpTestSuite *MongoDumpTestSuite) TearDownTest() {
	viper.Reset()
}

// mongorequest is a testcontainers.ContainerRequest for a basic mongodb testcontainer
var mongoRequest = testcontainers.ContainerRequest{
	Image:        mongoImage,
	ExposedPorts: []string{mongoPort},
	Env: map[string]string{
		"MONGODB_ROOT_USERNAME": mongoUser,
		"MONGODB_ROOT_PASSWORD": mongoPW,
		//"MONGODB_EXTRA_FLAGS": "--authenticationDatabase=admin",
	},
	WaitingFor: wait.ForLog(logString),
}

// execCommand executes a given command within a context and with specified arguments
func execCommand(ctx context.Context, cmd string, args ...string) ([]byte, error) {
	command := exec.CommandContext(ctx, cmd, args...)
	out, err := command.CombinedOutput()
	if err != nil {
		return out, err
	}
	return out, nil
}

// newMongoClient creates a mongo client connected to the provided commons.TestContainerSetup
func newMongoClient(target *commons.TestContainerSetup) (mongo.Client, error) {
	backupClientOptions := options.Client().ApplyURI(fmt.Sprintf("mongodb://%s:%s", target.Address,
		target.Port))
	clientAuth := options.Client().SetAuth(options.Credential{Username: mongoUser, Password: mongoPW})
	client, err := mongo.Connect(context.TODO(), backupClientOptions, clientAuth)
	if err != nil {

		return mongo.Client{}, err
	}

	err = client.Ping(context.TODO(), nil)
	if err != nil {
		return mongo.Client{}, err
	}
	return *client, nil
}

// createMongoConfig creates a brudi config for the `mongodump` command
func createMongoConfig(container commons.TestContainerSetup, useRestic bool, resticIP, resticPort, kind string) []byte {
	if !useRestic {
		return []byte(fmt.Sprintf(`
      %s:
        options:
          flags:
            host: %s
            port: %s
            username: %s
            password: %s
            gzip: true
            archive: %s
          additionalArgs: []
`, kind, container.Address, container.Port, mongoUser, mongoPW, backupPath))
	}
	return []byte(fmt.Sprintf(`
      %s:
        options:
          flags:
            host: %s
            port: %s
            username: %s
            password: %s
            gzip: true
            archive: %s
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
        restore:
          flags:
            target: "/"
          id: "latest"
`, kind, container.Address, container.Port, mongoUser, mongoPW, backupPath, resticIP, resticPort))
}

// prepareTestData creates test data and writes it into a database using the provided client
func prepareTestData(client *mongo.Client) ([]interface{}, error) {
	fooColl := TestColl{"Foo", 10}
	barColl := TestColl{"Bar", 13}
	gopherColl := TestColl{"Gopher", 42}
	testData := []interface{}{fooColl, barColl, gopherColl}
	collection := client.Database(dbName).Collection(collName)
	_, err := collection.InsertMany(context.TODO(), testData)
	if err != nil {
		return []interface{}{}, err
	}
	return testData, nil
}

// getResultsFromCursor iterates over a mongo.Cursor and returns the result
func getResultsFromCursor(cur *mongo.Cursor) ([]interface{}, error) {
	var results []interface{}
	for cur.Next(context.TODO()) {
		var elem TestColl
		err := cur.Decode(&elem)
		if err != nil {
			return []interface{}{}, err
		}
		results = append(results, elem)
	}
	err := cur.Close(context.TODO())
	if err != nil {
		return []interface{}{}, err
	}
	return results, nil
}

// mongoDoBackup performs a mongodump and returns the test data that was used for verification purposes
func mongoDoBackup(ctx context.Context, mongoDumpTestSuite *MongoDumpTestSuite, useRestic bool,
	resticContainer commons.TestContainerSetup) []interface{} {
	// create a mongodb-container to test backup function
	mongoBackupTarget, err := commons.NewTestContainerSetup(ctx, &mongoRequest, mongoPort)
	mongoDumpTestSuite.Require().NoError(err)
	defer func() {
		backErr := mongoBackupTarget.Container.Terminate(ctx)
		mongoDumpTestSuite.Require().NoError(backErr)
	}()

	// client to insert test data into database
	var backupClient mongo.Client
	//time.Sleep(30*time.Second)
	backupClient, err = newMongoClient(&mongoBackupTarget)
	mongoDumpTestSuite.Require().NoError(err)
	defer func() {
		clientErr := backupClient.Disconnect(ctx)
		mongoDumpTestSuite.Require().NoError(clientErr)
	}()

	// write test data into database and retain it for later assertion
	var testData []interface{}
	testData, err = prepareTestData(&backupClient)
	mongoDumpTestSuite.Require().NoError(err)

	testMongoConfig := createMongoConfig(mongoBackupTarget, useRestic, resticContainer.Address, resticContainer.Port, dumpKind)
	err = viper.ReadConfig(bytes.NewBuffer(testMongoConfig))
	mongoDumpTestSuite.Require().NoError(err)

	// perform backup action on mongodb-container
	err = source.DoBackupForKind(ctx, dumpKind, false, useRestic, false)
	mongoDumpTestSuite.Require().NoError(err)

	return testData
}

func mongoDoRestore(ctx context.Context, mongoDumpTestSuite *MongoDumpTestSuite, useRestic bool,
	resticContainer commons.TestContainerSetup) []interface{} {
	// setup a new mongodb-container which will be used to ensure data was backed up correctly
	mongoRestoreTarget, err := commons.NewTestContainerSetup(ctx, &mongoRequest, mongoPort)
	mongoDumpTestSuite.Require().NoError(err)
	defer func() {
		restoreErr := mongoRestoreTarget.Container.Terminate(ctx)
		mongoDumpTestSuite.Require().NoError(restoreErr)
	}()

	restoreMongoConfig := createMongoConfig(mongoRestoreTarget, useRestic, resticContainer.Address, resticContainer.Port, restoreKind)
	err = viper.ReadConfig(bytes.NewBuffer(restoreMongoConfig))
	mongoDumpTestSuite.Require().NoError(err)

	// use `mongorestore` to restore backed up data to new container
	err = source.DoRestoreForKind(ctx, restoreKind, false, useRestic, false)
	mongoDumpTestSuite.Require().NoError(err)

	restoreClient, err := newMongoClient(&mongoRestoreTarget)
	mongoDumpTestSuite.Require().NoError(err)
	defer func() {
		clientErr := restoreClient.Disconnect(ctx)
		mongoDumpTestSuite.Require().NoError(clientErr)
	}()

	// pull restored data from database
	restoredCollection := restoreClient.Database(dbName).Collection(collName)
	findOptions := options.Find()
	var cur *mongo.Cursor
	cur, err = restoredCollection.Find(context.TODO(), bson.D{{}}, findOptions)
	mongoDumpTestSuite.Require().NoError(err)

	var results []interface{}
	results, err = getResultsFromCursor(cur)
	mongoDumpTestSuite.Require().NoError(err)
	return results
}

// TestBasicMongoDBDump performs an integration test for the `mongodump` command
func (mongoDumpTestSuite *MongoDumpTestSuite) TestBasicMongoDBDump() {
	ctx := context.Background()

	defer func() {
		removeErr := os.Remove(backupPath)
		mongoDumpTestSuite.Require().NoError(removeErr)
	}()

	// backup test data with brudi and return the  data so it can be used to verify the restoration
	testData := mongoDoBackup(ctx, mongoDumpTestSuite, false, commons.TestContainerSetup{Port: "", Address: ""})

	results := mongoDoRestore(ctx, mongoDumpTestSuite, false, commons.TestContainerSetup{Port: "", Address: ""})
	// check if the original data was restored
	assert.DeepEqual(mongoDumpTestSuite.T(), testData, results)
}

// TestBasicMongoDBDumpRestic performs an integration test for the `mongodump` command with restic support
func (mongoDumpTestSuite *MongoDumpTestSuite) TestBasicMongoDBDumpRestic() {
	ctx := context.Background()

	// create a container running the restic rest-server
	resticContainer, err := commons.NewTestContainerSetup(ctx, &commons.ResticReq, commons.ResticPort)
	mongoDumpTestSuite.Require().NoError(err)
	defer func() {
		resticErr := resticContainer.Container.Terminate(ctx)
		mongoDumpTestSuite.Require().NoError(resticErr)
	}()

	testData := mongoDoBackup(ctx, mongoDumpTestSuite, true, resticContainer)

	// create new database container for restoration purposes
	var mongoRestoreTarget commons.TestContainerSetup
	mongoRestoreTarget, err = commons.NewTestContainerSetup(ctx, &mongoRequest, mongoPort)
	mongoDumpTestSuite.Require().NoError(err)
	defer func() {
		restoreErr := mongoRestoreTarget.Container.Terminate(ctx)
		mongoDumpTestSuite.Require().NoError(restoreErr)
	}()

	// pull data from restic-repository
	err = commons.DoResticRestore(ctx, resticContainer, dataDir)
	mongoDumpTestSuite.Require().NoError(err)

	results := mongoDoRestore(ctx, mongoDumpTestSuite, true, resticContainer)
	assert.DeepEqual(mongoDumpTestSuite.T(), testData, results)
}

func TestMongoDumpTestSuite(t *testing.T) {
	suite.Run(t, new(MongoDumpTestSuite))
}
