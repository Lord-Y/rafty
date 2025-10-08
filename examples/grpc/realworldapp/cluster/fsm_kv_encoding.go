package cluster

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"io"

	"github.com/Lord-Y/rafty"
)

// kvEncodeCommand permits to transform command receive from clients to binary language machine
func kvEncodeCommand(cmd kvCommand, w io.Writer) error {
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

// kvDecodeCommand permits to transform back command from binary language machine to clients
func kvDecodeCommand(data []byte) (kvCommand, error) {
	var cmd kvCommand
	buffer := bytes.NewBuffer(data)

	var kind uint32
	if err := binary.Read(buffer, binary.LittleEndian, &kind); err != nil {
		return cmd, err
	}
	cmd.Kind = commandKind(kind)

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

// kvApplyCommand will apply fsm to the k/v store
func (f *fsmState) kvApplyCommand(log *rafty.LogEntry) ([]byte, error) {
	decodedCmd, _ := kvDecodeCommand(log.Command)

	if rafty.LogKind(log.LogType) == rafty.LogCommandReadLeader {
		if decodedCmd.Kind == kvCommandGetAll {
			return f.memoryStore.usersEncoded()
		}
		return f.memoryStore.kvGet([]byte(decodedCmd.Key))
	}

	switch decodedCmd.Kind {
	case kvCommandSet:
		return nil, f.memoryStore.kvSet([]byte(decodedCmd.Key), []byte(decodedCmd.Value))

	case kvCommandGet:
		value, err := f.memoryStore.kvGet([]byte(decodedCmd.Key))
		if err != nil {
			return nil, err
		}
		return value, nil

	case kvCommandDelete:
		f.memoryStore.kvDelete([]byte(decodedCmd.Key))
	}

	return nil, nil
}

// unmarshalBinaryWithChecksumKVCommand permit to decode data in binary format
// by validating its checksum before moving further
func unmarshalBinaryWithChecksumKVCommand(data []byte) (userCommand, error) {
	if len(data) < 4 {
		return userCommand{}, rafty.ErrChecksumDataTooShort
	}

	body := data[:len(data)-4]
	checksum := binary.LittleEndian.Uint32(data[len(data)-4:])

	if crc32.ChecksumIEEE(body) != checksum {
		return userCommand{}, rafty.ErrChecksumMistmatch
	}

	return userDecodeCommand(body)
}

// kvsDecoded decodes binary data with checksum
func kvsDecoded(data []byte) ([]userCommand, error) {
	var cmds []userCommand
	buffer := bytes.NewBuffer(data)

	for buffer.Len() > 0 {
		var length uint32
		if err := binary.Read(buffer, binary.LittleEndian, &length); err != nil {
			return nil, err
		}

		// Read first 4 bytes to get entry size
		record := make([]byte, length)
		if _, err := io.ReadFull(buffer, record); err != nil {
			return nil, err
		}

		cmd, err := unmarshalBinaryWithChecksumKVCommand(record)
		if err != nil {
			return nil, err
		}
		cmds = append(cmds, cmd)
	}
	return cmds, nil
}
