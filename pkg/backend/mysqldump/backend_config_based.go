package mysqldump

import (
	"context"
	"fmt"
	"os"

	"github.com/pkg/errors"

	"github.com/mittwald/brudi/pkg/cli"
)

type ConfigBasedBackend struct {
	config Config
}

func NewConfigBasedBackend() (*ConfigBasedBackend, error) {
	backend := &ConfigBasedBackend{
		config: Config{
			ClientOptions: make(map[string]interface{}),
		},
	}

	err := backend.config.InitFromViper()
	if err != nil {
		return nil, err
	}

	return backend, nil
}

func (b *ConfigBasedBackend) CreateBackup(ctx context.Context) error {
	dumpOut, err := os.Create(b.config.Out)
	if err != nil {
		return errors.WithStack(err)
	}

	defer func() {
		_ = dumpOut.Close()
	}()

	flags := &CliFlags{DefaultsFile: b.config.clientMyCnfPath}

	cmd := cli.CommandType{
		Binary: "mysqldump",
		Args:   cli.StructToCLI(flags),
	}

	var out []byte
	out, err = cli.Run(ctx, cmd)
	if err != nil {
		return errors.WithStack(fmt.Errorf("%+v - %+v", out, err))
	}

	_, err = dumpOut.Write(out)
	if err != nil {
		return errors.WithStack(err)
	}

	return nil
}

func (b *ConfigBasedBackend) GetBackupPath() string {
	return b.config.Out
}

func (b *ConfigBasedBackend) GetHostname() string {
	return fmt.Sprintf("%s", b.config.ClientOptions["host"])
}
