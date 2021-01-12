package testcli

import (
	"context"
	"github.com/mittwald/brudi/pkg/cli"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/suite"
	"io/ioutil"
	"os"
	"testing"
)

const testFile = "/tmp/gzip_testfile.txt"
const binary = "gzip"

type CliTestSuite struct {
	suite.Suite
}

func (cliTestSuite *CliTestSuite) TestGzipFile() {
	err := createTestFile()

	var fileName string
	fileName, err = cli.GzipFile(testFile)
	defer func() {
		remError := os.Remove(fileName)
		if remError != nil {
			log.WithError(remError).Error("failed to remove test archive from TestValidGzipFile()")
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

const cupcakes = "Cupcake ipsum dolor sit amet. Cotton candy pudding oat cake muffin candy canes halvah dessert tiramisu donut. " +
	"Marzipan dessert sweet wafer ice cream tootsie roll tootsie roll tart marshmallow. " +
	"Caramels apple pie croissant chupa chups macaroon cupcake. Cookie chocolate bar dragée biscuit." +
	" Sweet chocolate icing sweet cake. Tiramisu toffee tart lollipop halvah. Cake soufflé bonbon donut donut. " +
	"Jelly sweet roll pastry tart cotton candy jelly beans marshmallow. Candy canes jelly-o donut ice cream. " +
	"Jelly-o cookie oat cake cake ice cream soufflé. Chupa chups gummi bears chocolate cake chocolate cake jelly-o jelly-o " +
	"oat cake ice cream. Dessert wafer pudding."

func createTestFile() error {
	content := []byte(cupcakes)
	err := ioutil.WriteFile(testFile, content, 0644)
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}
