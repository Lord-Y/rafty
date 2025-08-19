package rafty

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/Lord-Y/rafty/raftypb"
)

var (
	metadataFile  string = "metadata.json"
	dataStateDir  string = "wal"
	dataStateFile string = "state.bin"
)

// metadata is a struct holding all requirements
// to persist node metadata
type metadata struct {
	// Id of the current raft server
	Id string `json:"id"`

	// CurrentTerm is latest term seen during the voting campaign
	CurrentTerm uint64 `json:"currentTerm"`

	// VotedFor is the node the current node voted for during the election campaign
	VotedFor string `json:"votedFor"`

	// LastApplied is the index of the highest log entry applied to the current raft server
	LastApplied uint64 `json:"lastApplied"`

	// LastAppliedConfig is the index of the highest log entry configuration applied
	// to the current raft server
	LastAppliedConfigIndex uint64 `json:"lastAppliedConfigIndex"`

	// LastAppliedConfigTerm is the term of the highest log entry configuration applied
	// to the current raft server
	LastAppliedConfigTerm uint64 `json:"lastAppliedConfigTerm"`

	// Configuration hold server members
	Configuration configuration `json:"configuration"`
}

// metaFile hold all requirements to manage file metadata
type metaFile struct {
	rafty        *Rafty
	fullFilename string
	file         *os.File
}

// dataFile hold all requirements to manage file data
type dataFile struct {
	rafty        *Rafty
	fullFilename string
	file         *os.File
}

// storage hold both metaFile and dataFile
type storage struct {
	metadata metaFile
	data     dataFile
}

// createDirectoryIfNotExist permits to check if a directory exist
// and create it if not. An error will be return if there is any
func createDirectoryIfNotExist(d string, perm fs.FileMode) error {
	if fileinfo, err := os.Stat(d); os.IsNotExist(err) {
		if err := os.MkdirAll(d, perm); err != nil {
			return err
		}
	} else {
		if fileinfo == nil || !fileinfo.IsDir() {
			return fmt.Errorf("%s is not a directory", d)
		}
		return nil
	}
	return nil
}

// newStorage instantiate rafty with default storage configuration
func (r *Rafty) newStorage() (metadata metaFile, data dataFile, err error) {
	if r.options.DataDir == "" {
		return
	}

	datadir := filepath.Join(r.options.DataDir, dataStateDir)
	if err := createDirectoryIfNotExist(datadir, 0750); err != nil {
		return metadata, data, fmt.Errorf("fail to create directory %s: %w", datadir, err)
	}

	metadata.fullFilename = filepath.Join(r.options.DataDir, metadataFile)
	if metadata.file, err = os.OpenFile(metadata.fullFilename, os.O_RDWR|os.O_CREATE, 0644); err != nil {
		return metadata, data, fmt.Errorf("fail to create file %s: %w", metadata.fullFilename, err)
	}

	data.fullFilename = filepath.Join(datadir, dataStateFile)
	if data.file, err = os.OpenFile(data.fullFilename, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0644); err != nil {
		return metadata, data, fmt.Errorf("fail to create file %s: %w", data.fullFilename, err)
	}

	return
}

// restore allow to restore both metadata and data from disk
func (r storage) restore() error {
	var err error
	if err = r.metadata.restore(); err != nil {
		return err
	}
	if err = r.data.restore(); err != nil {
		return err
	}
	return nil
}

// restore allow to restore logs from disk
func (r metaFile) restore() error {
	if r.file == nil {
		r.rafty.mu.Lock()
		defer r.rafty.mu.Unlock()
		for _, server := range r.rafty.options.Peers {
			r.rafty.configuration.ServerMembers = append(r.rafty.configuration.ServerMembers, peer{Address: server.Address})
		}
		return nil
	}

	var (
		err    error
		result []byte
	)
	_, _ = r.file.Seek(0, 0)
	if result, err = io.ReadAll(r.file); err != nil {
		return err
	}

	if len(result) > 0 {
		var data metadata
		if err = json.Unmarshal(result, &data); err != nil {
			return err
		}

		r.rafty.mu.Lock()
		r.rafty.id = data.Id
		r.rafty.currentTerm.Store(data.CurrentTerm)
		r.rafty.votedFor = data.VotedFor
		r.rafty.lastApplied.Store(data.LastApplied)
		r.rafty.lastAppliedConfigIndex.Store(data.LastAppliedConfigIndex)
		r.rafty.lastAppliedConfigTerm.Store(data.LastAppliedConfigTerm)
		if r.rafty.options.BootstrapCluster && !r.rafty.isBootstrapped.Load() && data.LastAppliedConfigIndex > 0 {
			r.rafty.isBootstrapped.Store(true)
		}
		if len(data.Configuration.ServerMembers) > 0 {
			r.rafty.configuration.ServerMembers = data.Configuration.ServerMembers
		} else {
			for _, server := range r.rafty.options.Peers {
				r.rafty.configuration.ServerMembers = append(r.rafty.configuration.ServerMembers, peer{Address: server.Address})
			}
		}
		r.rafty.mu.Unlock()
		return nil
	}

	r.rafty.mu.Lock()
	for _, server := range r.rafty.options.Peers {
		r.rafty.configuration.ServerMembers = append(r.rafty.configuration.ServerMembers, peer{Address: server.Address})
	}
	r.rafty.mu.Unlock()

	return nil
}

// store allow to persist data logs on disk
func (r metaFile) store() error {
	r.rafty.wg.Add(1)
	defer r.rafty.wg.Done()
	if r.file == nil {
		return nil
	}

	r.rafty.mu.Lock()
	peers := r.rafty.configuration.ServerMembers
	r.rafty.mu.Unlock()

	data := metadata{
		Id:                     r.rafty.id,
		CurrentTerm:            r.rafty.currentTerm.Load(),
		VotedFor:               r.rafty.votedFor,
		LastApplied:            r.rafty.lastApplied.Load(),
		LastAppliedConfigIndex: r.rafty.lastAppliedConfigIndex.Load(),
		LastAppliedConfigTerm:  r.rafty.lastAppliedConfigTerm.Load(),
		Configuration: configuration{
			ServerMembers: peers,
		},
	}

	var (
		err    error
		result []byte
	)
	if result, err = json.Marshal(data); err != nil {
		return err
	}

	_ = r.file.Truncate(0)
	_, _ = r.file.Seek(0, 0)
	if _, err = r.file.Write(result); err != nil {
		return err
	}

	if err = r.file.Sync(); err != nil {
		return err
	}
	return nil
}

// close allow to close opened file
func (r metaFile) close() {
	if r.file != nil {
		_ = r.file.Close()
	}
}

// restore allow to restore data from disk
func (r dataFile) restore() error {
	if r.file == nil {
		return nil
	}

	var err error
	_, _ = r.file.Seek(0, 0)
	scanner := bufio.NewScanner(r.file)
	for scanner.Scan() {
		if len(scanner.Bytes()) > 0 {
			var data *raftypb.LogEntry
			if data, err = unmarshalBinaryWithChecksum(scanner.Bytes()); err != nil && err != io.EOF {
				return err
			}
			if data != nil {
				r.rafty.logs.appendEntries([]*raftypb.LogEntry{data}, true)
			}
		}
	}

	// Check for scanning errors
	if err := scanner.Err(); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// store allow to persist data logs on disk
func (r dataFile) store(entry *raftypb.LogEntry) error {
	r.rafty.wg.Add(1)
	defer r.rafty.wg.Done()
	if r.file == nil {
		return nil
	}

	logEntry := &logEntry{
		FileFormat: uint8(entry.FileFormat),
		Tombstone:  uint8(entry.Tombstone),
		LogType:    uint8(entry.LogType),
		Timestamp:  entry.Timestamp,
		Term:       entry.Term,
		Index:      entry.Index,
		Command:    entry.Command,
	}

	var err error
	buffer, bufferChecksum := new(bytes.Buffer), new(bytes.Buffer)
	if err = marshalBinary(logEntry, buffer); err != nil {
		return err
	}

	if err = marshalBinaryWithChecksum(buffer, bufferChecksum); err != nil {
		return err
	}

	writer := bufio.NewWriter(r.file)
	if _, err = writer.Write(bufferChecksum.Bytes()); err != nil {
		return err
	}

	// Write a newline after each struct
	if _, err = writer.WriteString("\n"); err != nil {
		return err
	}

	// Flush() only flushes the buffered data to the OS file cache
	if err = writer.Flush(); err != nil {
		return err
	}

	// Sync() force the OS to flush its cache to disk guaranteeing the data
	// is physically written to disk.
	if err = r.file.Sync(); err != nil {
		return err
	}
	return nil
}

// close allow to close opened file
func (r dataFile) close() {
	if r.file != nil {
		_ = r.file.Close()
	}
}

// close allow to close both opened file metadata and data
func (r storage) close() {
	r.metadata.close()
	r.data.close()
}
