package rafty

import (
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParsePeers(t *testing.T) {
	assert := assert.New(t)

	addr := net.TCPAddr{
		IP:   net.ParseIP("127.0.0.1"),
		Port: int(GRPCPort),
	}
	peers := []Peer{
		{
			Address: "127.0.0.1",
		},
		{
			Address: "127.0.0.2",
		},
		{
			Address: "127.0.0.3:50053",
		},
	}

	badPeers1 := []Peer{
		{
			Address: "127.0.0.1:50051",
		},
		{
			Address: "127.0.0.2:50052",
		},
		{
			Address: "127.0.0.3:aaaa",
		},
	}

	badPeers2 := []Peer{
		{
			Address: "127.0.0.1:50051",
		},
		{
			Address: "127.0.0.2:50052",
		},
		{
			Address: "[127.0.0.2:b",
		},
		{
			Address: "[foo]:[bar]baz",
		},
		{
			Address: "127.0.0.4::aaaa",
		},
	}

	s := NewServer(addr)
	id := "abe35d4f-787e-4262-9894-f6475ed81028"
	s.ID = id
	s.Peers = peers

	err := s.parsePeers()
	assert.Nil(err)

	s.Peers = badPeers1
	err = s.parsePeers()
	assert.Error(err)

	s.Peers = badPeers2
	err = s.parsePeers()
	assert.Error(err)
}
