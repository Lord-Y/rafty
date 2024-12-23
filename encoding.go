package rafty

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/Lord-Y/rafty/raftypb"
)

// encodeCommand permits to transform command receive from clients to binary language machine
func (r *Rafty) encodeCommand(cmd command) []byte {
	buf := bytes.NewBuffer(nil)

	err := buf.WriteByte(byte(cmd.kind))
	if err != nil {
		panic(err)
	}

	err = binary.Write(buf, binary.LittleEndian, uint64(len(cmd.key)))
	if err != nil {
		panic(err)
	}

	buf.WriteString(cmd.key)

	err = binary.Write(buf, binary.LittleEndian, uint64(len(cmd.value)))
	if err != nil {
		panic(err)
	}

	buf.WriteString(cmd.value)

	return buf.Bytes()
}

// decodeCommand permits to transform back command from binary language machine to clients
func (r *Rafty) decodeCommand(data []byte) command {
	var cmd command

	// here are some useful links to understand binary encoding/decoding
	// https://nakabonne.dev/posts/binary-encoding-go/
	// https://www.gobeyond.dev/encoding-binary/
	// uint32 = 4 bytes
	// uint64 = 8 bytes
	// kind   0 4 should be 4 bytes but cast to 1 bit in our purpose with byte(cmd.kind)
	// key    4 12
	// value 12 20
	cmd.kind = commandKind(data[0])
	// 9 = 1 bit + 8 bytes
	keyLen := binary.LittleEndian.Uint64(data[1:9])
	maxKeyLen := 9 + keyLen
	cmd.key = string(data[9:maxKeyLen])

	switch cmd.kind {
	case commandSet, commandGet:
		// 9+keyLen+8 means 9 bytes + len(key) + 8 bytes
		valueLen := binary.LittleEndian.Uint64(data[maxKeyLen : 9+keyLen+8])
		// can be written string(data[9+keyLen+8:])
		cmd.value = string(data[9+keyLen+8 : 9+keyLen+8+valueLen])
	}

	return cmd
}

func (r *Rafty) encodePersistentData(entry *logEntry) []byte {
	buffer := bytes.NewBuffer(nil)

	err := binary.Write(buffer, binary.LittleEndian, entry.FileFormat)
	if err != nil {
		r.Logger.Fatal().Err(err).Msgf("Fail to encode log entry")
	}

	err = binary.Write(buffer, binary.LittleEndian, entry.Tombstone)
	if err != nil {
		r.Logger.Fatal().Err(err).Msgf("Fail to encode log entry")
	}

	err = binary.Write(buffer, binary.LittleEndian, entry.TimeStamp)
	if err != nil {
		r.Logger.Fatal().Err(err).Msgf("Fail to encode log entry")
	}

	err = binary.Write(buffer, binary.LittleEndian, entry.Term)
	if err != nil {
		r.Logger.Fatal().Err(err).Msgf("Fail to encode log entry")
	}

	err = binary.Write(buffer, binary.LittleEndian, uint64(len(entry.Command)))
	if err != nil {
		r.Logger.Fatal().Err(err).Msgf("Fail to encode log entry")
	}

	_, err = buffer.Write(entry.Command)
	if err != nil {
		r.Logger.Fatal().Err(err).Msgf("Fail to encode log entry")
	}

	_, err = buffer.WriteString("\n")
	if err != nil {
		r.Logger.Fatal().Err(err).Msgf("Fail to encode log entry")
	}

	return buffer.Bytes()
}

func (r *Rafty) decodePersistentData(data []byte) *raftypb.LogEntry {
	var entry logEntry
	buffer := bytes.NewBuffer(data)
	if err := binary.Read(buffer, binary.LittleEndian, &entry.FileFormat); err != nil {
		if err != io.EOF {
			r.Logger.Fatal().Err(err).Msgf("Fail to decode data")
		}
	}

	if err := binary.Read(buffer, binary.LittleEndian, &entry.Tombstone); err != nil {
		if err != io.EOF {
			r.Logger.Fatal().Err(err).Msgf("Fail to decode data")
		}
	}

	if err := binary.Read(buffer, binary.LittleEndian, &entry.TimeStamp); err != nil {
		if err != io.EOF {
			r.Logger.Fatal().Err(err).Msgf("Fail to decode data")
		}
	}

	if err := binary.Read(buffer, binary.LittleEndian, &entry.Term); err != nil {
		if err != io.EOF {
			r.Logger.Fatal().Err(err).Msgf("Fail to decode data")
		}
	}

	// Limit the reader to the bytes of last field only
	var commandByte uint64
	if err := binary.Read(buffer, binary.LittleEndian, &commandByte); err != nil {
		if err != io.EOF {
			r.Logger.Fatal().Err(err).Msgf("Fail to decode data")
		}
	}

	limitReader := io.LimitReader(buffer, int64(len(data)-10))
	if commandByte > 0 {
		entry.Command = make([]byte, commandByte)
		if _, err := limitReader.Read(entry.Command); err != nil {
			if err != io.EOF {
				r.Logger.Fatal().Err(err).Msgf("Fail to decode data")
			}
		}
	}

	logEntry := raftypb.LogEntry{
		FileFormat: uint32(entry.FileFormat),
		Tombstone:  uint32(entry.Tombstone),
		TimeStamp:  entry.TimeStamp,
		Term:       entry.Term,
		Command:    entry.Command,
	}
	return &logEntry
}
