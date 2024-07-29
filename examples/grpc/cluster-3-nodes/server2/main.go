package main

import (
	"net"

	"github.com/Lord-Y/rafty"
)

func main() {
	addr := net.TCPAddr{
		IP:   net.ParseIP("127.0.0.2"),
		Port: 50052,
	}
	peers := []*rafty.Peer{
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
	s.ID = "229fc9de-a8f7-4d21-964f-f23a2cc20eff"
	s.Peers = peers

	err := s.Start()
	if err != nil {
		s.Logger.Fatal().Err(err).Msg("Fail to serve gRPC server")
	}
}
