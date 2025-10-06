package commands

import (
	"context"

	"realworldapp/cluster"
	"realworldapp/logger"

	"github.com/urfave/cli/v3"
)

func Server() *cli.Command {
	var app cluster.Cluster

	return &cli.Command{
		Name:  "server",
		Usage: "Allow us to start instance",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "host",
				Aliases:     []string{"h"},
				Value:       "127.0.0.1",
				Usage:       "Address used by this instance",
				Destination: &app.Host,
			},
			&cli.IntFlag{
				Name:        "http-port",
				Aliases:     []string{"hp"},
				Usage:       "http port to use",
				Value:       15080,
				Destination: &app.HTTPPort,
			},
			&cli.IntFlag{
				Name:        "grpc-port",
				Aliases:     []string{"gp"},
				Usage:       "grpc port to use",
				Value:       15050,
				Destination: &app.GRPCPort,
			},
			&cli.StringSliceFlag{
				Name:        "member",
				Aliases:     []string{"m"},
				Usage:       "GRPC address of the member that will join the cluster, this flag is repeatable",
				Required:    true,
				Destination: &app.Members,
			},
		},
		Action: func(context.Context, *cli.Command) error {
			app.Logger = logger.NewLogger()

			return app.Start()
		},
	}
}
