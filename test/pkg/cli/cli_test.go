package testcli

import (
	"context"
	"github.com/mittwald/brudi/pkg/cli"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/suite"
	"io/ioutil"
	"net/http"
	"os"
	"testing"
)

const testFile = "/tmp/gzip_testfile.txt"
const binary = "gzip"
const expectedFileType = "text/plain; charset=utf-8"

// permissions for test file, readable by all, writable by user
const fileMode = 0644

type CliTestSuite struct {
	suite.Suite
}

// TestCheckAndGunzipFile tests if a file is correectly handled by cli.CheckAndGunzipFile
func (cliTestSuite *CliTestSuite) TestCheckAndGunzipFile() {
	err := createTestFile()
	cliTestSuite.Require().NoError(err)

	// Check if uncompressed file is handled correctly, i.e. if correct name gets passed through
	var fileName string
	fileName, err = cli.CheckAndGunzipFile(testFile)
	cliTestSuite.Require().NoError(err)
	cliTestSuite.Assert().Equal(testFile, fileName)

	// zip the file to test handling of compressed files
	var fileNameZipped string
	fileNameZipped, err = cli.GzipFile(testFile)
	defer func() {
		remError := os.Remove(fileNameZipped)
		if remError != nil {
			log.WithError(remError).Error("failed to remove test archive from TestCheckAndGunzipFile()")
		}
	}()
	cliTestSuite.Require().NoError(err)

	// unzip file
	fileName, err = cli.CheckAndGunzipFile(fileNameZipped)
	cliTestSuite.Require().NoError(err)

	// check file type of the unzipped file to ensure the file was actually unzipped
	var file *os.File
	file, err = os.Open(fileName)
	if err != nil {
		log.WithError(err).Error("failed to open unzipped file in TestCheckAndGunzipFile")
	}
	defer func() {
		fileErr := file.Close()
		if fileErr != nil {
			log.WithError(fileErr).Errorf("failed to close source file %s in TestCheckAndGunzipFile", fileName)
		}
	}()
	headerBytes := make([]byte, 512)
	_, err = file.Read(headerBytes)
	if err != nil {
		log.WithError(err).Error("failed to read file header in TestCheckAndGunzipFile")
	}

	// check correct file type and file name
	fileType := http.DetectContentType(headerBytes)
	cliTestSuite.Assert().Equal(expectedFileType, fileType)
	cliTestSuite.Assert().Equal(testFile, fileName)

}

// TestGzipFile checks if file generated by cli.GzipFile is a valid gzip compressed file
func (cliTestSuite *CliTestSuite) TestGzipFile() {
	err := createTestFile()
	cliTestSuite.Require().NoError(err)

	var fileName string
	fileName, err = cli.GzipFile(testFile)
	defer func() {
		remError := os.Remove(fileName)
		if remError != nil {
			log.WithError(remError).Error("failed to remove test archive from TestGzipFile()")
		}
	}()
	cliTestSuite.Require().NoError(err)

	cmd := cli.CommandType{
		Binary: binary,
		Args:   []string{"-t", fileName},
	}
	_, err = cli.Run(context.TODO(), cmd)
	cliTestSuite.Require().NoError(err)
}

func TestCliTestSuite(t *testing.T) {
	suite.Run(t, new(CliTestSuite))
}

// creates the test file for TestGzipFile
func createTestFile() error {
	content := []byte(cupcakes)
	err := ioutil.WriteFile(testFile, content, fileMode)
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}

// content for test file
const cupcakes = `Cupcake ipsum dolor sit amet. Cotton candy pudding oat cake muffin candy canes halvah dessert tiramisu donut.
Marzipan dessert sweet wafer ice cream tootsie roll tootsie roll tart marshmallow.
Caramels apple pie croissant chupa chups macaroon cupcake. Cookie chocolate bar dragée biscuit.
Sweet chocolate icing sweet cake. Tiramisu toffee tart lollipop halvah. Cake soufflé bonbon donut donut.
Jelly sweet roll pastry tart cotton candy jelly beans marshmallow. Candy canes jelly-o donut ice cream.
Jelly-o cookie oat cake cake ice cream soufflé. Chupa chups gummi bears chocolate cake chocolate cake jelly-o jelly-o
oat cake ice cream. Dessert wafer pudding.`
