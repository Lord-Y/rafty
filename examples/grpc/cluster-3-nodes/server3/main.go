package main

import (
	"net"
	"os"
	"path/filepath"

	"github.com/Lord-Y/rafty"
)

func main() {
	addr := net.TCPAddr{
		IP:   net.ParseIP("127.0.0.3"),
		Port: 50053,
	}
	peers := []rafty.Peer{
		{
			Address: "127.0.0.1",
		},
		{
			Address: "127.0.0.2:50052",
		},
		{
			Address: "127.0.0.3:50053",
		},
	}

	s := rafty.NewServer(addr)
	id := "775c0bce-f3ed-47d0-9b44-0e0909d48e1a"
	s.ID = id
	s.Peers = peers
	s.DataDir = filepath.Join(os.TempDir(), "rafty", id)

	err := s.Start()
	if err != nil {
		s.Logger.Fatal().Err(err).Msg("Fail to serve gRPC server")
	}
}
