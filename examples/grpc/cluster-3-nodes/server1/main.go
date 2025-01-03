package main

import (
	"net"
	"os"
	"path/filepath"

	"github.com/Lord-Y/rafty"
)

func main() {
	addr := net.TCPAddr{
		IP:   net.ParseIP("127.0.0.1"),
		Port: int(rafty.GRPCPort),
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
	id := "abe35d4f-787e-4262-9894-f6475ed81028"
	s.ID = id
	s.Peers = peers
	s.DataDir = filepath.Join(os.TempDir(), "rafty", id)

	err := s.Start()
	if err != nil {
		s.Logger.Fatal().Err(err).Msg("Fail to serve gRPC server")
	}
}
