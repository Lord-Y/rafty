package cluster

import (
	"bytes"
	"encoding/binary"
)

// decodeCommand permits to get the command kind that will then be used by user or kv decoding funcs
func decodeCommand(data []byte) (commandKind, error) {
	buffer := bytes.NewBuffer(data)

	var kind uint32
	if err := binary.Read(buffer, binary.LittleEndian, &kind); err != nil {
		return 0, err
	}

	return commandKind(kind), nil
}
