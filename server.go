package rafty

import (
	"log"
	"net"
	"os"

	"github.com/Lord-Y/rafty/grpcrequests"
	"github.com/pkg/errors"
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

	// Quit represents the channel that will be used to stop the server
	quit chan os.Signal

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
}

// ServerNew instantiate default configuration of the gRPC server to later start or stop it
func ServerNew(address net.TCPAddr) *Server {
	return &Server{
		Address: address,
		quit:    make(chan os.Signal, 1),
	}
}

// Start permits to start the gRPC server with the provided configuration
func (g *Server) Start() error {
	listener, err := net.Listen(g.Address.Network(), g.Address.String())
	if err != nil {
		return errors.Wrap(err, "Fail to listen gRPC server")
	}
	g.listener = listener

	g.server = grpc.NewServer()
	grpcrequests.RegisterGreeterServer(g.server, &ProtobufSVC{})
	reflection.Register(g.server)

	log.Println("Starting gRPC server at", g.Address.String())
	err = g.server.Serve(listener)
	if err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
	return nil
}

// Stop permits to stop the gRPC server with the provided configuration
func (g *Server) Stop() {
	if g.server == nil {
		return
	}
	g.server.GracefulStop()
	g.listener.Close()
	g.server = nil
	log.Println("Stopped gRPC server successful")
}
