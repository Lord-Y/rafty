package rafty

import (
	"context"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Lord-Y/rafty/grpcrequests"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
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

var kaep = keepalive.EnforcementPolicy{
	MinTime:             5 * time.Second, // If a client pings more than once every 5 seconds, terminate the connection
	PermitWithoutStream: true,            // Allow pings even when there are no active streams
}

var kasp = keepalive.ServerParameters{
	MaxConnectionIdle: 15 * time.Second, // If a client is idle for 15 seconds, send a GOAWAY
	MaxConnectionAge:  10 * time.Minute, // If any connection is alive for more than 30 seconds, send a GOAWAY
	// MaxConnectionAge:      30 * time.Second, // If any connection is alive for more than 30 seconds, send a GOAWAY
	MaxConnectionAgeGrace: 5 * time.Second, // Allow 5 seconds for pending RPCs to complete before forcibly closing connections
	Time:                  5 * time.Second, // Ping the client if it is idle for 5 seconds to ensure the connection is still active
	Timeout:               1 * time.Second, // Wait 1 second for the ping ack before assuming the connection is dead
}

var kacp = keepalive.ClientParameters{
	Time:                10 * time.Second, // send pings every 10 seconds if there is no activity
	Timeout:             time.Second,      // wait 1 second for ping ack before considering the connection dead
	PermitWithoutStream: true,             // send pings even without active streams
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
		for {
			select {
			case <-ctx.Done():
				// stop go routine when os signal is receive or ctrl+c
				// and gracefully stop
				g.Stop()
			}
		}
	}()

	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
		go g.Rafty.Start()
		g.server.Serve(listener)
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
