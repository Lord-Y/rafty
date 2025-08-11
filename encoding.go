package rafty

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"hash/crc32"
	"io"

	"github.com/Lord-Y/rafty/raftypb"
)

// encodeCommand permits to transform command receive from clients to binary language machine
func encodeCommand(cmd Command, w io.Writer) error {
	if err := binary.Write(w, binary.LittleEndian, uint32(cmd.Kind)); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, uint64(len(cmd.Key))); err != nil {
		return err
	}
	if _, err := w.Write([]byte(cmd.Key)); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, uint64(len(cmd.Value))); err != nil {
		return err
	}
	if _, err := w.Write([]byte(cmd.Value)); err != nil {
		return err
	}
	return nil
}

// decodeCommand permits to transform back command from binary language machine to clients
func decodeCommand(data []byte) (Command, error) {
	var cmd Command
	buffer := bytes.NewBuffer(data)

	var kind uint32
	if err := binary.Read(buffer, binary.LittleEndian, &kind); err != nil {
		return cmd, err
	}
	cmd.Kind = CommandKind(kind)

	var keyLen uint64
	if err := binary.Read(buffer, binary.LittleEndian, &keyLen); err != nil {
		return cmd, err
	}

	key := make([]byte, keyLen)
	if _, err := buffer.Read(key); err != nil {
		return cmd, err
	}
	cmd.Key = string(key)

	var valueLen uint64
	if err := binary.Read(buffer, binary.LittleEndian, &valueLen); err != nil {
		return cmd, err
	}
	value := make([]byte, valueLen)
	if _, err := buffer.Read(value); err != nil {
		return cmd, err
	}
	cmd.Value = string(value)

	return cmd, nil
}

// marshalBinary permit to encode data in binary format
func marshalBinary(entry *logEntry, w io.Writer) error {
	if err := binary.Write(w, binary.LittleEndian, entry.FileFormat); err != nil {
		return err
	}

	if err := binary.Write(w, binary.LittleEndian, entry.Tombstone); err != nil {
		return err
	}

	if err := binary.Write(w, binary.LittleEndian, entry.LogType); err != nil {
		return err
	}

	if err := binary.Write(w, binary.LittleEndian, entry.Timestamp); err != nil {
		return err
	}

	if err := binary.Write(w, binary.LittleEndian, entry.Term); err != nil {
		return err
	}

	if err := binary.Write(w, binary.LittleEndian, entry.Index); err != nil {
		return err
	}

	if err := binary.Write(w, binary.LittleEndian, uint64(len(entry.Command))); err != nil {
		return err
	}

	if _, err := w.Write(entry.Command); err != nil {
		return err
	}

	return nil
}

// unmarshalBinary permit to decode data in binary format
func unmarshalBinary(data []byte) (*raftypb.LogEntry, error) {
	var entry logEntry
	buffer := bytes.NewBuffer(data)

	if err := binary.Read(buffer, binary.LittleEndian, &entry.FileFormat); err != nil {
		return nil, err
	}

	if err := binary.Read(buffer, binary.LittleEndian, &entry.Tombstone); err != nil {
		return nil, err
	}

	if err := binary.Read(buffer, binary.LittleEndian, &entry.LogType); err != nil {
		return nil, err
	}

	if err := binary.Read(buffer, binary.LittleEndian, &entry.Timestamp); err != nil {
		return nil, err
	}

	if err := binary.Read(buffer, binary.LittleEndian, &entry.Term); err != nil {
		return nil, err
	}

	if err := binary.Read(buffer, binary.LittleEndian, &entry.Index); err != nil {
		return nil, err
	}

	var commandLen uint64
	if err := binary.Read(buffer, binary.LittleEndian, &commandLen); err != nil {
		return nil, err
	}

	entry.Command = make([]byte, commandLen)
	if _, err := buffer.Read(entry.Command); err != nil {
		return nil, err
	}

	logEntry := raftypb.LogEntry{
		FileFormat: uint32(entry.FileFormat),
		Tombstone:  uint32(entry.Tombstone),
		LogType:    uint32(entry.LogType),
		Timestamp:  entry.Timestamp,
		Term:       entry.Term,
		Index:      entry.Index,
		Command:    entry.Command,
	}
	return &logEntry, nil
}

// marshalBinaryWithChecksum permit to encode data in binary format
// with checksum before being written to disk
func marshalBinaryWithChecksum(buffer *bytes.Buffer, w io.Writer) error {
	checksum := crc32.ChecksumIEEE(buffer.Bytes())

	if _, err := w.Write(buffer.Bytes()); err != nil {
		return err
	}

	if err := binary.Write(w, binary.LittleEndian, checksum); err != nil {
		return err
	}

	return nil
}

// unmarshalBinaryWithChecksum permit to decode data in binary format
// by validating its checksum before moving further
func unmarshalBinaryWithChecksum(data []byte) (entry *raftypb.LogEntry, err error) {
	if len(data) < 4 {
		return nil, ErrChecksumDataTooShort
	}

	body := data[:len(data)-4]
	checksum := binary.LittleEndian.Uint32(data[len(data)-4:])

	if crc32.ChecksumIEEE(body) != checksum {
		return nil, ErrChecksumMistmatch
	}

	return unmarshalBinary(body)
}

// encodePeers permits to encode peers and return bytes
func encodePeers(data []peer) (result []byte) {
	// checking error is irrelevant here as it will always be nil
	// in this case
	result, _ = json.Marshal(data)
	return result
}

// decodePeers permits to decode peers and return bytes
func decodePeers(data []byte) (result []peer, err error) {
	if err = json.Unmarshal(data, &result); err != nil {
		return
	}
	return
}

func encodeUint64ToBytes(value uint64) []byte {
	buffer := make([]byte, 8)
	binary.LittleEndian.PutUint64(buffer, value)
	return buffer
}

func decodeUint64ToBytes(value []byte) uint64 {
	return binary.LittleEndian.Uint64(value)
}
