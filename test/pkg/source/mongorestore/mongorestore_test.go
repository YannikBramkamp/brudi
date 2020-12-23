package mongorestore_test

import (
	"bytes"
	"context"
	"fmt"
	"github.com/docker/go-connections/nat"
	"github.com/mittwald/brudi/pkg/source"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"gotest.tools/assert"
	"os"
	"strings"
	"testing"
)

const mongoPort = "27017/tcp"
const backupPath = "/tmp/dump.tar.gz"
const ResticPort = "8000/tcp"

type MongoRestoreTestSuite struct {
	suite.Suite
}

type TestColl struct {
	Name string
	Age  int
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

var mongoRequest = testcontainers.ContainerRequest{
	Image:        "mongo:latest",
	ExposedPorts: []string{mongoPort},
	Env: map[string]string{
		"MONGO_INITDB_ROOT_USERNAME": "root",
		"MONGO_INITDB_ROOT_PASSWORD": "mongodbroot",
	},
}

func (mongoRestoreTestSuite *MongoRestoreTestSuite) SetupTest() {
	viper.Reset()
	viper.SetConfigType("yaml")
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.AutomaticEnv()
}

func (mongoRestoreTestSuite *MongoRestoreTestSuite) TearDownTest() {
	viper.Reset()
}

// newMongoClient creates a mongo client connected to the database specified by the provided TestContainerSetup
func newMongoClient(target *TestContainerSetup) (mongo.Client, error) {
	backupClientOptions := options.Client().ApplyURI(fmt.Sprintf("mongodb://%s:%s", target.Address,
		target.Port))
	clientAuth := options.Client().SetAuth(options.Credential{Username: "root", Password: "mongodbroot"})

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

// createMongoRestoreConfig creates a brudi config for the mongorestore command
func createMongoRestoreConfig(container TestContainerSetup, userestic bool, resticIP, resticport string) []byte {
	return []byte(fmt.Sprintf(`
     mongorestore:
       options:
         flags:
           host: %s
           port: %s
           username: root
           password: mongodbroot
           gzip: true
           archive: %s
         additionalArgs: []
     restic:
        global:
          flags:
            repo: rest:http://%s:%s/
        restore:
          flags:
            target: "/"
          id: "latest"
`, container.Address, container.Port, backupPath, resticIP, resticport))
}

// createMongoConfig creates a brudi config for the mongodump command
func createMongoConfig(container TestContainerSetup, useRestic bool, resticIP, resticPort string) []byte {
	if !useRestic {
		return []byte(fmt.Sprintf(`
      mongodump:
        options:
          flags:
            host: %s
            port: %s
            username: root
            password: mongodbroot
            gzip: true
            archive: %s
          additionalArgs: []
  `, container.Address, container.Port, backupPath))
	}
	return []byte(fmt.Sprintf(`
      mongodump:
        options:
          flags:
            host: %s
            port: %s
            username: root
            password: mongodbroot
            gzip: true
            archive: %s
          additionalArgs: []
      restic:
        global:
          flags:
            repo: rest:http://%s:%s/
        restore:
          flags:
            target: "/"
          id: "latest"
        forget:
          flags:
            keepLast: 1
            keepHourly: 0
            keepDaily: 0
            keepWeekly: 0
            keepMonthly: 0
            keepYearly: 0
`, container.Address, container.Port, backupPath, resticIP, resticPort))
}

// prepareTestData creates test data and writes it into a database using the provided client
func prepareTestData(client *mongo.Client) ([]interface{}, error) {
	fooColl := TestColl{"Foo", 10}
	barColl := TestColl{"Bar", 13}
	gopherColl := TestColl{"Gopher", 42}
	testData := []interface{}{fooColl, barColl, gopherColl}
	collection := client.Database("test").Collection("testColl")
	_, err := collection.InsertMany(context.TODO(), testData)
	if err != nil {
		return []interface{}{}, err
	}
	return testData, nil
}

// getResultsFromCursor iterates over a mongo.Cursor and returns the result.It also closes the cursor when done.
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

func (mongoRestoreTestSuite *MongoRestoreTestSuite) TestBasicMongoRestore() {
	ctx := context.Background()

	// create a mongo container to test backup function
	mongoBackupTarget, err := NewTestContainerSetup(ctx, &mongoRequest, mongoPort)
	mongoRestoreTestSuite.Require().NoError(err)

	backupClient, err := newMongoClient(&mongoBackupTarget)
	mongoRestoreTestSuite.Require().NoError(err)

	// write test data into database and retain it for later assertion
	testData, err := prepareTestData(&backupClient)
	mongoRestoreTestSuite.Require().NoError(err)

	err = backupClient.Disconnect(context.TODO())
	mongoRestoreTestSuite.Require().NoError(err)

	testMongoConfig := createMongoConfig(mongoBackupTarget, false, "", "")
	err = viper.ReadConfig(bytes.NewBuffer(testMongoConfig))
	mongoRestoreTestSuite.Require().NoError(err)

	// perform backup action on first mongo container
	err = source.DoBackupForKind(ctx, "mongodump", false, false, false)
	mongoRestoreTestSuite.Require().NoError(err)

	err = mongoBackupTarget.Container.Terminate(ctx)
	mongoRestoreTestSuite.Require().NoError(err)

	// setup a new mongo container which will be used to ensure data was backed up correctly
	mongoRestoreTarget, err := NewTestContainerSetup(ctx, &mongoRequest, mongoPort)
	mongoRestoreTestSuite.Require().NoError(err)

	testMongoRestoreConfig := createMongoRestoreConfig(mongoRestoreTarget, false, "", "")
	err = viper.ReadConfig(bytes.NewBuffer(testMongoRestoreConfig))
	mongoRestoreTestSuite.Require().NoError(err)

	// restore data using restore function
	err = source.DoRestoreForKind(ctx, "mongorestore", false, false, false)
	mongoRestoreTestSuite.Require().NoError(err)

	restoreClient, err := newMongoClient(&mongoRestoreTarget)
	mongoRestoreTestSuite.Require().NoError(err)

	restoredCollection := restoreClient.Database("test").Collection("testColl")

	findOptions := options.Find()
	cur, err := restoredCollection.Find(context.TODO(), bson.D{{}}, findOptions)
	mongoRestoreTestSuite.Require().NoError(err)

	err = os.Remove(backupPath)
	mongoRestoreTestSuite.Require().NoError(err)

	results, err := getResultsFromCursor(cur)
	mongoRestoreTestSuite.Require().NoError(err)

	// check if the original data was restored
	assert.DeepEqual(mongoRestoreTestSuite.T(), testData, results)

	err = mongoRestoreTarget.Container.Terminate(ctx)
	mongoRestoreTestSuite.Require().NoError(err)
	err = restoreClient.Disconnect(context.TODO())
	mongoRestoreTestSuite.Require().NoError(err)
}

func (mongoRestoreTestSuite *MongoRestoreTestSuite) TestMongoRestoreRestic() {
	ctx := context.Background()

	// create a mongo container to test backup function
	mongoBackupTarget, err := NewTestContainerSetup(ctx, &mongoRequest, mongoPort)
	mongoRestoreTestSuite.Require().NoError(err)

	resticContainer, err := NewTestContainerSetup(ctx, &ResticReq, ResticPort)
	mongoRestoreTestSuite.Require().NoError(err)

	backupClient, err := newMongoClient(&mongoBackupTarget)
	mongoRestoreTestSuite.Require().NoError(err)

	// write test data into database and retain it for later assertion
	testData, err := prepareTestData(&backupClient)
	mongoRestoreTestSuite.Require().NoError(err)

	err = backupClient.Disconnect(context.TODO())
	mongoRestoreTestSuite.Require().NoError(err)

	testMongoConfig := createMongoConfig(mongoBackupTarget, true, resticContainer.Address, resticContainer.Port)
	err = viper.ReadConfig(bytes.NewBuffer(testMongoConfig))
	mongoRestoreTestSuite.Require().NoError(err)

	// perform backup action on first mongo container
	err = source.DoBackupForKind(ctx, "mongodump", false, true, false)
	mongoRestoreTestSuite.Require().NoError(err)

	err = mongoBackupTarget.Container.Terminate(ctx)
	mongoRestoreTestSuite.Require().NoError(err)

	// setup a new mongo container which will be used to ensure data was backed up correctly
	mongoRestoreTarget, err := NewTestContainerSetup(ctx, &mongoRequest, mongoPort)
	mongoRestoreTestSuite.Require().NoError(err)

	testMongoRestoreConfig := createMongoRestoreConfig(mongoRestoreTarget, true, resticContainer.Address, resticContainer.Port)
	err = viper.ReadConfig(bytes.NewBuffer(testMongoRestoreConfig))
	mongoRestoreTestSuite.Require().NoError(err)

	// restore data using restore function
	err = source.DoRestoreForKind(ctx, "mongorestore", false, true, false)
	mongoRestoreTestSuite.Require().NoError(err)

	restoreClient, err := newMongoClient(&mongoRestoreTarget)
	mongoRestoreTestSuite.Require().NoError(err)

	restoredCollection := restoreClient.Database("test").Collection("testColl")

	findOptions := options.Find()
	cur, err := restoredCollection.Find(context.TODO(), bson.D{{}}, findOptions)
	mongoRestoreTestSuite.Require().NoError(err)

	err = os.Remove(backupPath)
	mongoRestoreTestSuite.Require().NoError(err)

	results, err := getResultsFromCursor(cur)
	mongoRestoreTestSuite.Require().NoError(err)

	// check if the original data was restored
	assert.DeepEqual(mongoRestoreTestSuite.T(), testData, results)

	err = mongoRestoreTarget.Container.Terminate(ctx)
	mongoRestoreTestSuite.Require().NoError(err)
	err = restoreClient.Disconnect(context.TODO())
	mongoRestoreTestSuite.Require().NoError(err)
	err = resticContainer.Container.Terminate(ctx)
	mongoRestoreTestSuite.Require().NoError(err)

	// cleanup
	err = os.RemoveAll("data")
	mongoRestoreTestSuite.Require().NoError(err)
}

func TestMongoRestoreTestSuite(t *testing.T) {
	suite.Run(t, new(MongoRestoreTestSuite))
}
