package main

import (
	"context"
	"os"
	"realworldapp/commands"
	"realworldapp/logger"

	"github.com/urfave/cli/v3"
)

func main() {
	cmd := cli.Command{
		Name:                  "realworldapp",
		Usage:                 "An example of rafty implementation",
		Description:           "An example of rafty implementation",
		EnableShellCompletion: true,
		Commands: []*cli.Command{
			commands.Server(),
		},
	}

	if err := cmd.Run(context.Background(), os.Args); err != nil {
		logger.NewLogger().Fatal().Err(err).Msg("Error occured while executing the program")
	}
}
