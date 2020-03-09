package cmd

import (
	"strings"

	"github.com/mitchellh/go-homedir"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	// Used for flags.
	cfgFile   string
	useRestic bool
	cleanup   bool

	rootCmd = &cobra.Command{
		Use:   "brudi",
		Short: "Easy backup creation",
		Long: `brudi's target is easy, incremental and encrypted backup creation for different backends (file, mongoDB, mysql, etc.).
 After backing up things, brudi uploads them to an S3 object storage.`,
		Version: commit,
	}
)

func init() {
	cobra.OnInitialize(initConfig)
	rootCmd.PersistentFlags().BoolVar(&useRestic, "restic", false, "backup result with restic and push it to s3")

	rootCmd.PersistentFlags().BoolVar(&cleanup, "cleanup", false, "cleanup backup files afterwards")

	rootCmd.PersistentFlags().StringVarP(&cfgFile, "config", "c", "", "config file (default is ${HOME}/.brudi.yaml)")
}

// Execute executes the root command.
func Execute() error {
	return rootCmd.Execute()
}

func initConfig() {
	if cfgFile != "" {
		viper.SetConfigFile(cfgFile)
	} else {
		home, err := homedir.Dir()
		if err != nil {
			log.WithError(err).Fatal("unable to determine homedir for current user")
		}

		viper.AddConfigPath(home)
		viper.SetConfigName(".brudi.yaml")
	}

	viper.SetConfigType("yaml")

	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.AutomaticEnv()

	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			// cfg file not found; ignore error if desired
		} else {
			log.WithError(err).Fatal("failed while reading config")
		}
	}

	if len(viper.ConfigFileUsed()) > 0 {
		log.WithField("config", viper.ConfigFileUsed()).Info("config loaded")
	}
}
