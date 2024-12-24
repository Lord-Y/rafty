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

	cmd := command{
		kind:  commandGet,
		key:   "a",
		value: "b",
	}
	enc := node.encodeCommand(cmd)
	assert.NotNil(enc)
	dec := node.decodeCommand(enc)
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

	cmd := &logEntry{}
	enc := node.marshalBinary(cmd)
	assert.NotNil(enc)
	dec := node.unmarshalBinary(enc)
	assert.Equal(&raftypb.LogEntry{}, dec)

	now := uint32(time.Now().Unix())
	data := []byte("a=b")
	cmd.FileFormat = 1
	cmd.Term = 1
	cmd.TimeStamp = now
	cmd.Command = data

	enc = node.marshalBinary(cmd)
	assert.NotNil(enc)
	dec = node.unmarshalBinary(enc)
	assert.Equal(cmd.FileFormat, uint8(dec.FileFormat))
	assert.Equal(cmd.Term, dec.Term)
	assert.Equal(cmd.TimeStamp, dec.TimeStamp)
	assert.Equal(cmd.Tombstone, uint8(dec.Tombstone))
	assert.Equal(cmd.Command, dec.Command)
}
