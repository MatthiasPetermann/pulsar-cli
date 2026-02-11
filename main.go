package main

import (
	"os"

	"petermann-digital.de/pulsar-cli/v2/internal/cli"

	"github.com/sirupsen/logrus"
)

func main() {
	configureLogging()

	if err := cli.NewRootCmd().Execute(); err != nil {
		logrus.Fatal(err)
	}
}

// configureLogging centralizes logger defaults for all commands.
func configureLogging() {
	logrus.SetFormatter(&logrus.TextFormatter{
		ForceColors:   true,
		FullTimestamp: true,
	})
	logrus.SetOutput(os.Stderr)
	logrus.SetLevel(logrus.InfoLevel)
}
