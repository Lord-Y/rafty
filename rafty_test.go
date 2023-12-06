package rafty

import (
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestStart(t *testing.T) {
	assert := assert.New(t)

	s1Addr := net.TCPAddr{
		IP:   net.ParseIP("127.0.0.5"),
		Port: int(GRPCPort),
	}
	s1Peers := []*Peer{
		{
			Address: "127.0.0.5",
		},
		{
			Address: "127.0.0.5:50052",
		},
		{
			Address: "127.0.0.5:50053",
		},
	}

	s1 := NewServer(s1Addr)
	s1.Rafty.ID = "abe35d4f-787e-4262-9894-f6475ed81028"
	s1.Rafty.Peers = s1Peers

	s2Addr := net.TCPAddr{
		IP:   net.ParseIP("127.0.0.5"),
		Port: 50052,
	}
	s2Peers := []*Peer{
		{
			Address: "127.0.0.5",
		},
		{
			Address: "127.0.0.5:50052",
		},
		{
			Address: "127.0.0.5:50053",
		},
	}

	s2 := NewServer(s2Addr)
	s2.Rafty.ID = "229fc9de-a8f7-4d21-964f-f23a2cc20eff"
	s2.Rafty.Peers = s2Peers

	s3Addr := net.TCPAddr{
		IP:   net.ParseIP("127.0.0.5"),
		Port: 50053,
	}
	s3Peers := []*Peer{
		{
			Address: "127.0.0.5",
		},
		{
			Address: "127.0.0.5:50052",
		},
		{
			Address: "127.0.0.5:50053",
		},
	}

	s3 := NewServer(s3Addr)
	s3.Rafty.ID = "775c0bce-f3ed-47d0-9b44-0e0909d48e1a"
	s3.Rafty.Peers = s3Peers

	go func() {
		err := s1.Start()
		assert.NoError(err)
	}()

	go func() {
		err := s2.Start()
		assert.NoError(err)
	}()

	go func() {
		err := s3.Start()
		assert.NoError(err)
	}()

	time.Sleep(30 * time.Second)
	go func() {
		s1.Stop()
	}()

	time.Sleep(30 * time.Second)
	go func() {
		err := s1.Start()
		assert.NoError(err)
		// force start again to catch error address already binded
		err = s1.Start()
		assert.Error(err)
	}()

	time.Sleep(30 * time.Second)
	go func() {
		s2.Stop()
	}()

	time.Sleep(30 * time.Second)
	go func() {
		err := s2.Start()
		assert.NoError(err)
	}()

	time.Sleep(30 * time.Second)
	go func() {
		s3.Stop()
	}()

	time.Sleep(30 * time.Second)
	go func() {
		err := s3.Start()
		assert.NoError(err)
	}()

	time.Sleep(20 * time.Second)
	go func() {
		s1.Stop()
		s2.Stop()
		s3.Stop()
	}()
}
