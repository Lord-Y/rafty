package main

import (
	"net"

	"log"

	"github.com/Lord-Y/rafty"
)

func main() {
	addr := net.TCPAddr{
		IP:   net.ParseIP(rafty.GRPCAddress),
		Port: int(rafty.GRPCPort),
	}

	s := rafty.NewServer(addr)
	err := s.Start()
	if err != nil {
		log.Println(err)
	}
}
