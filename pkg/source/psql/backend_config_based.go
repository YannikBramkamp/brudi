package psql

import (
	"context"
	"fmt"
	"os"

	"github.com/pkg/errors"

	"github.com/mittwald/brudi/pkg/cli"
)

type ConfigBasedBackend struct {
	cfg *Config
}

func NewConfigBasedBackend() (*ConfigBasedBackend, error) {
	config := &Config{
		&Options{
			Flags:          &Flags{},
			AdditionalArgs: []string{},
			SourceFile:     "",
		},
	}

	err := config.InitFromViper()
	if err != nil {
		return nil, err
	}

	return &ConfigBasedBackend{cfg: config}, nil
}

func (b *ConfigBasedBackend) RestoreBackup(ctx context.Context) error {
	args := append(cli.StructToCLI(b.cfg.Options.Flags), b.cfg.Options.AdditionalArgs...)
	args = append(args, []string{"-c", fmt.Sprintf("source %s", b.cfg.Options.SourceFile)}...)
	cmd := cli.CommandType{
		Binary: binary,
		Args:   append(cli.StructToCLI(b.cfg.Options.Flags), b.cfg.Options.AdditionalArgs...),
	}
	out, err := cli.RunWithFile(ctx, cmd, b.cfg.Options.SourceFile)
	if err != nil {
		return errors.WithStack(fmt.Errorf("%+v - %s", err, out))
	}

	return nil
}

func (b *ConfigBasedBackend) GetBackupPath() string {
	return b.cfg.Options.SourceFile
}

func (b *ConfigBasedBackend) GetHostname() string {
	return b.cfg.Options.Flags.Host
}

func (b *ConfigBasedBackend) CleanUp() error {
	return os.Remove(b.GetBackupPath())
}
