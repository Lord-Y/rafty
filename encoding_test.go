package rafty

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/Lord-Y/rafty/logger"
	"github.com/stretchr/testify/assert"
)

type failWriter struct {
	failOn int
	count  int
}

func (fw *failWriter) Write(p []byte) (int, error) {
	fw.count++
	if fw.count == fw.failOn {
		return 0, errors.New("forced write error")
	}
	return len(p), nil
}

func TestEncoding_EncodeDecodeCommand(t *testing.T) {
	assert := assert.New(t)

	cc := clusterConfig{
		t:           t,
		clusterSize: 1,
		testName:    "EncodeDecodeCommand",
	}
	cc.assert = assert
	cc.cluster = cc.makeCluster()
	node := cc.cluster[0]
	logger := logger.NewLogger().With().Str("logProvider", "rafty_test").Logger()
	node.Logger = &logger

	cmd := Command{
		Kind:  CommandGet,
		Key:   "a",
		Value: "b",
	}

	// Testing error on Kind write
	w := &failWriter{failOn: 1}
	buffer := new(bytes.Buffer)

	t.Run("encode", func(t *testing.T) {
		assert.Error(encodeCommand(cmd, w))

		// Testing error on Key length write
		w = &failWriter{failOn: 2}
		assert.Error(encodeCommand(cmd, w))

		// Testing error on Key write
		w = &failWriter{failOn: 3}
		assert.Error(encodeCommand(cmd, w))

		// Testing error on Value length write
		w = &failWriter{failOn: 4}
		assert.Error(encodeCommand(cmd, w))

		// Testing error on Value write
		w = &failWriter{failOn: 5}
		assert.Error(encodeCommand(cmd, w))

		// No errors expected here
		assert.Nil(encodeCommand(cmd, buffer))
		assert.NotNil(buffer.Bytes())
	})

	t.Run("decode", func(t *testing.T) {
		// Error reading Kind
		_, err := decodeCommand([]byte{})
		assert.Error(err)

		// Error reading Key length
		buf := new(bytes.Buffer)
		_ = binary.Write(buf, binary.LittleEndian, uint32(1)) // Kind
		// No KeyLen
		_, err = decodeCommand(buf.Bytes())
		assert.Error(err)

		// Error reading Key
		buf.Reset()
		_ = binary.Write(buf, binary.LittleEndian, uint32(1)) // Kind
		_ = binary.Write(buf, binary.LittleEndian, uint64(3)) // KeyLen
		// No Key bytes
		_, err = decodeCommand(buf.Bytes())
		assert.Error(err)

		// Error reading Value length
		buf.Reset()
		_ = binary.Write(buf, binary.LittleEndian, uint32(1)) // Kind
		_ = binary.Write(buf, binary.LittleEndian, uint64(3)) // KeyLen
		buf.Write([]byte("abc"))                              // Key
		// No ValueLen
		_, err = decodeCommand(buf.Bytes())
		assert.Error(err)

		// Error reading Value
		buf.Reset()
		_ = binary.Write(buf, binary.LittleEndian, uint32(1)) // Kind
		_ = binary.Write(buf, binary.LittleEndian, uint64(3)) // KeyLen
		buf.Write([]byte("abc"))                              // Key
		_ = binary.Write(buf, binary.LittleEndian, uint64(2)) // ValueLen
		// No Value bytes
		_, err = decodeCommand(buf.Bytes())
		assert.Error(err)

		// No errors expected here
		dec, err := decodeCommand(buffer.Bytes())
		assert.Nil(err)
		assert.Equal(cmd, dec)
	})
}

func TestEncoding_MarshalUnmarshalBinary(t *testing.T) {
	assert := assert.New(t)

	cc := clusterConfig{
		t:           t,
		clusterSize: 1,
		testName:    "MarshallUnmarshallBinary",
	}
	cc.assert = assert
	cc.cluster = cc.makeCluster()
	node := cc.cluster[0]
	logger := logger.NewLogger().With().Str("logProvider", "rafty_test").Logger()
	node.Logger = &logger

	for index := range 2 {
		cmd := &logEntry{}
		// Testing error on FileFormat write
		w := &failWriter{failOn: 1}
		assert.Error(MarshalBinary(cmd, w))

		// Testing error on Tombstone write
		w = &failWriter{failOn: 2}
		assert.Error(MarshalBinary(cmd, w))

		// Testing error on LogType write
		w = &failWriter{failOn: 3}
		assert.Error(MarshalBinary(cmd, w))

		// Testing error on Timestamp write
		w = &failWriter{failOn: 4}
		assert.Error(MarshalBinary(cmd, w))

		// Testing error on Term write
		w = &failWriter{failOn: 5}
		assert.Error(MarshalBinary(cmd, w))

		// Testing error on Index write
		w = &failWriter{failOn: 6}
		assert.Error(MarshalBinary(cmd, w))

		// Testing error on Command length write
		w = &failWriter{failOn: 7}
		assert.Error(MarshalBinary(cmd, w))

		// Testing error on Command write
		w = &failWriter{failOn: 8}
		assert.Error(MarshalBinary(cmd, w))

		// No errors expected here
		buffer := new(bytes.Buffer)
		assert.Nil(MarshalBinary(cmd, buffer))
		enc := buffer.Bytes()
		assert.NotNil(enc)
		dec, err := UnmarshalBinary(enc)
		assert.Nil(err)
		assert.Equal(&logEntry{Command: []byte{}}, dec)

		now := uint32(time.Now().Unix())
		data := []byte("a=b")
		cmd.FileFormat = uint32(index)
		cmd.LogType = uint32(index)
		cmd.Term = 1
		cmd.Index = uint64(index)
		cmd.Timestamp = now
		cmd.Command = data

		// No errors expected here
		buffer = new(bytes.Buffer)
		assert.Nil(MarshalBinary(cmd, buffer))
		enc = buffer.Bytes()
		assert.NotNil(enc)

		// Testing error on length write
		w = &failWriter{failOn: 1}
		assert.Error(MarshalBinaryWithChecksum(buffer, w))

		// Testing error on buffer write
		w = &failWriter{failOn: 2}
		assert.Error(MarshalBinaryWithChecksum(buffer, w))

		// Testing error on checksum write
		w = &failWriter{failOn: 3}
		assert.Error(MarshalBinaryWithChecksum(buffer, w))

		// Testing error on data too short
		_, err = UnmarshalBinaryWithChecksum([]byte(""))
		assert.Error(err)

		// Testing error on CRC32 checksum mistmatch
		_, err = UnmarshalBinaryWithChecksum(enc)
		assert.Error(err)

		// No errors expected here
		bufferChecksum := new(bytes.Buffer)
		assert.Nil(MarshalBinaryWithChecksum(buffer, bufferChecksum))

		var length uint32
		assert.Nil(binary.Read(bufferChecksum, binary.LittleEndian, &length))

		record := make([]byte, length)
		_, err = io.ReadFull(bufferChecksum, record)
		assert.Nil(err)
		_, err = UnmarshalBinaryWithChecksum(record)
		assert.Nil(err)

		dec, err = UnmarshalBinary(enc)
		assert.Nil(err)
		assert.Equal(cmd.FileFormat, dec.FileFormat)
		assert.Equal(cmd.Tombstone, dec.Tombstone)
		assert.Equal(cmd.LogType, dec.LogType)
		assert.Equal(cmd.Timestamp, dec.Timestamp)
		assert.Equal(cmd.Term, dec.Term)
		assert.Equal(cmd.Index, dec.Index)
		assert.Equal(cmd.Command, dec.Command)

		// Error reading FileFormat
		_, err = UnmarshalBinary([]byte{})
		assert.Error(err)

		// Error reading Tombstone
		buf := new(bytes.Buffer)
		_ = binary.Write(buf, binary.LittleEndian, uint32(1)) // FileFormat
		_, err = UnmarshalBinary(buf.Bytes())
		assert.Error(err)

		// Error reading LogType
		buf.Reset()
		_ = binary.Write(buf, binary.LittleEndian, uint32(1)) // FileFormat
		_ = binary.Write(buf, binary.LittleEndian, uint32(0)) // Tombstone
		_, err = UnmarshalBinary(buf.Bytes())
		assert.Error(err)

		// Error reading Timestamp
		buf.Reset()
		_ = binary.Write(buf, binary.LittleEndian, uint32(1)) // FileFormat
		_ = binary.Write(buf, binary.LittleEndian, uint32(0)) // Tombstone
		_ = binary.Write(buf, binary.LittleEndian, uint32(0)) // LogType
		_, err = UnmarshalBinary(buf.Bytes())
		assert.Error(err)

		// Error reading Term
		buf.Reset()
		_ = binary.Write(buf, binary.LittleEndian, uint32(1)) // FileFormat
		_ = binary.Write(buf, binary.LittleEndian, uint32(0)) // Tombstone
		_ = binary.Write(buf, binary.LittleEndian, uint32(0)) // LogType
		_ = binary.Write(buf, binary.LittleEndian, uint32(1)) // Timestamp
		_, err = UnmarshalBinary(buf.Bytes())
		assert.Error(err)

		// Error reading Index
		buf.Reset()
		_ = binary.Write(buf, binary.LittleEndian, uint32(1)) // FileFormat
		_ = binary.Write(buf, binary.LittleEndian, uint32(0)) // Tombstone
		_ = binary.Write(buf, binary.LittleEndian, uint32(0)) // LogType
		_ = binary.Write(buf, binary.LittleEndian, uint32(1)) // Timestamp
		_ = binary.Write(buf, binary.LittleEndian, uint64(1)) // Term
		_, err = UnmarshalBinary(buf.Bytes())
		assert.Error(err)

		// Error reading Command length
		buf.Reset()
		_ = binary.Write(buf, binary.LittleEndian, uint32(1)) // FileFormat
		_ = binary.Write(buf, binary.LittleEndian, uint32(0)) // Tombstone
		_ = binary.Write(buf, binary.LittleEndian, uint32(0)) // LogType
		_ = binary.Write(buf, binary.LittleEndian, uint32(1)) // Timestamp
		_ = binary.Write(buf, binary.LittleEndian, uint64(1)) // Term
		_ = binary.Write(buf, binary.LittleEndian, uint64(1)) // Index
		_, err = UnmarshalBinary(buf.Bytes())
		assert.Error(err)

		// Error reading Command bytes
		buf.Reset()
		_ = binary.Write(buf, binary.LittleEndian, uint32(1)) // FileFormat
		_ = binary.Write(buf, binary.LittleEndian, uint32(0)) // Tombstone
		_ = binary.Write(buf, binary.LittleEndian, uint32(0)) // LogType
		_ = binary.Write(buf, binary.LittleEndian, uint32(1)) // Timestamp
		_ = binary.Write(buf, binary.LittleEndian, uint64(1)) // Term
		_ = binary.Write(buf, binary.LittleEndian, uint64(1)) // Index
		_ = binary.Write(buf, binary.LittleEndian, uint64(5)) // Command length

		// Not enough bytes for Command
		_, err = UnmarshalBinary(buf.Bytes())
		assert.Error(err)
	}
}

func TestEncoding_EncodeDecodePeers(t *testing.T) {
	assert := assert.New(t)

	s := basicNodeSetup()
	defer func() {
		assert.Nil(s.logStore.Close())
	}()
	peers, _ := s.getPeers()
	peers = append(peers, Peer{Address: "127.0.0.1:60000", ID: "xyz"})
	encodedPeers := encodePeers(peers)
	assert.NotNil(encodedPeers)

	decodedPeers, err := decodePeers(encodedPeers)
	assert.Nil(err)
	assert.NotNil(decodedPeers)

	_, err = decodePeers([]byte(`a=b`))
	assert.Error(err)
}

func TestEncoding_EncodeDecodeUint64(t *testing.T) {
	assert := assert.New(t)

	value := uint64(1)
	enc := encodeUint64ToBytes(value)
	assert.NotNil(enc)
	assert.Equal(value, decodeUint64ToBytes(enc))
}
