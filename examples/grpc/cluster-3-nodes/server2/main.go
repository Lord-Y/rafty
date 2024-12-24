package main

import (
	"net"
	"os"
	"path/filepath"

	"github.com/Lord-Y/rafty"
)

func main() {
	addr := net.TCPAddr{
		IP:   net.ParseIP("127.0.0.2"),
		Port: 50052,
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
	id := "229fc9de-a8f7-4d21-964f-f23a2cc20eff"
	s.ID = id
	s.Peers = peers
	s.PersistDataOnDisk = true
	s.DataDir = filepath.Join(os.TempDir(), "rafty", id)

	err := s.Start()
	if err != nil {
		s.Logger.Fatal().Err(err).Msg("Fail to serve gRPC server")
	}
}
