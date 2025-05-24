package rafty

import (
	"testing"
	"time"

	"github.com/Lord-Y/rafty/logger"
	"github.com/Lord-Y/rafty/raftypb"
	"github.com/stretchr/testify/assert"
)

func TestEncodeDecodeCommand(t *testing.T) {
	assert := assert.New(t)

	cc := clusterConfig{
		t:           t,
		clusterSize: 1,
	}
	cc.cluster = cc.makeCluster()
	node := cc.cluster[0]
	logger := logger.NewLogger().With().Str("logProvider", "rafty").Logger()
	node.Logger = &logger

	cmd := Command{
		Kind:  CommandGet,
		Key:   "a",
		Value: "b",
	}
	enc, err := encodeCommand(cmd)
	assert.Nil(err)
	assert.NotNil(enc)
	dec, err := decodeCommand(enc)
	assert.Nil(err)
	assert.Equal(cmd, dec)
}

func TestMarshallUnmarshallBinary(t *testing.T) {
	assert := assert.New(t)

	cc := clusterConfig{
		t:           t,
		clusterSize: 1,
	}
	cc.cluster = cc.makeCluster()
	node := cc.cluster[0]
	logger := logger.NewLogger().With().Str("logProvider", "rafty").Logger()
	node.Logger = &logger

	for index := range 2 {
		cmd := &logEntry{}
		enc, err := marshalBinary(cmd)
		assert.Nil(err)
		assert.NotNil(enc)
		dec, err := unmarshalBinary(enc)
		assert.Nil(err)
		assert.Equal(&raftypb.LogEntry{Command: []byte{}}, dec)

		now := uint32(time.Now().Unix())
		data := []byte("a=b")
		cmd.FileFormat = uint8(index)
		cmd.LogType = uint8(index)
		cmd.Term = 1
		cmd.Index = uint64(index)
		cmd.Timestamp = now
		cmd.Command = data

		enc, err = marshalBinary(cmd)
		assert.Nil(err)
		assert.NotNil(enc)
		dec, err = unmarshalBinary(enc)
		assert.Nil(err)
		assert.Equal(cmd.FileFormat, uint8(dec.FileFormat))
		assert.Equal(cmd.LogType, uint8(dec.LogType))
		assert.Equal(cmd.Term, dec.Term)
		assert.Equal(cmd.Index, dec.Index)
		assert.Equal(cmd.Timestamp, dec.Timestamp)
		assert.Equal(cmd.Tombstone, uint8(dec.Tombstone))
		assert.Equal(cmd.Command, dec.Command)
	}
}

func TestEncodeDecodePeers(t *testing.T) {
	assert := assert.New(t)

	s := basicNodeSetup()
	err := s.parsePeers()
	assert.Nil(err)
	peers, _ := s.getPeers()
	peers = append(peers, peer{Address: "127.0.0.1:6000", ID: "xyz"})
	encodedPeers, err := encodePeers(peers)
	assert.Nil(err)
	assert.NotNil(encodedPeers)

	decodedPeers, err := decodePeers(encodedPeers)
	assert.Nil(err)
	assert.NotNil(decodedPeers)

	_, err = decodePeers([]byte(`a=b`))
	assert.Error(err)
}
