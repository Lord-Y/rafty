package cluster

import (
	"bytes"
	"encoding/binary"
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
