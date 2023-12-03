package rafty

import (
	"context"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/Lord-Y/rafty/grpcrequests"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

const (
	// GRPCAddress defines the default address to run the grpc server
	GRPCAddress string = "127.0.0.1"

	// GRPCPort define the default port to run the grpc server
	GRPCPort uint16 = 50051
)

type Server struct {
	// Address consists of the default address of the server
	Address net.TCPAddr

	// ready represents the channel that will be used to the server is ready to serve clients
	ready chan struct{}

	// Rafty expose raft config
	Rafty

	// grpc listener
	listener net.Listener

	// grpc server
	server *grpc.Server
}

// ProtobufSVC is use to implement grpcrequests.RegisterGreeterServer
type ProtobufSVC struct {
	grpcrequests.UnimplementedGreeterServer
	grpcrequests.RaftyServer

	// Logger expose zerolog so it can be override
	Logger *zerolog.Logger

	rafty *Rafty
}

// NewServer instantiate default configuration of the gRPC server to later start or stop it
func NewServer(address net.TCPAddr) *Server {
	return &Server{
		Address: address,
		ready:   make(chan struct{}),
		Rafty:   *NewRafty(),
	}
}

// Start permits to start the gRPC server with the provided configuration
func (g *Server) Start() error {
	g.Rafty.Address = g.Address
	listener, err := net.Listen(g.Address.Network(), g.Address.String())
	if err != nil {
		return errors.Wrap(err, "Fail to listen gRPC server")
	}
	g.listener = listener

	g.server = grpc.NewServer(
	// grpc.KeepaliveEnforcementPolicy(kaep),
	// grpc.KeepaliveParams(kasp),
	)
	pbSVC := &ProtobufSVC{
		Logger: g.Rafty.Logger,
		rafty:  &g.Rafty,
	}

	grpcrequests.RegisterRaftyServer(g.server, pbSVC)
	reflection.Register(g.server)

	g.Rafty.Logger.Info().Msgf("Starting gRPC server at %s", g.Address.String())

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	g.Rafty.signalCtx = ctx

	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
		// stop go routine when os signal is receive or ctrl+c
		// and gracefully stop
		<-ctx.Done()
		g.Stop()
	}()

	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
		go g.Rafty.Start()
		err := g.server.Serve(listener)
		if err != nil {
			g.Logger.Fatal().Err(err).Msg("fail to start grpc server")
		}
	}()
	g.wg.Wait()
	return nil
}

// Stop permits to stop the gRPC server and Rafty with the provided configuration
func (g *Server) Stop() {
	if g.server == nil {
		return
	}
	g.server.GracefulStop()
	g.listener.Close()
	g.server = nil
	g.Rafty.Logger.Info().Msg("Stopping gRPC server successful")
	os.Exit(0)
}
