package rafty

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"

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
	LastAppliedConfig uint64 `json:"lastAppliedConfig"`

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
	if _, err := os.Stat(d); os.IsNotExist(err) {
		if err := os.MkdirAll(d, perm); err != nil {
			return err
		}
		return nil
	}
	return nil
}

// newStorage instantiate rafty with default storage configuration
func (r *Rafty) newStorage() (metadata metaFile, data dataFile) {
	if !r.options.PersistDataOnDisk {
		return
	}
	if r.options.DataDir == "" {
		return
	}

	datadir := filepath.Join(r.options.DataDir, dataStateDir)
	if err := createDirectoryIfNotExist(datadir, 0750); err != nil {
		r.Logger.Fatal().Err(err).Msgf("Fail to create directory %s", datadir)
	}

	var err error
	metadata.fullFilename = filepath.Join(r.options.DataDir, metadataFile)
	if metadata.file, err = os.OpenFile(metadata.fullFilename, os.O_RDWR|os.O_CREATE, 0644); err != nil {
		r.Logger.Fatal().Err(err).Msgf("Fail to create file %s", metadata.fullFilename)
	}

	data.fullFilename = filepath.Join(datadir, dataStateFile)
	if data.file, err = os.OpenFile(data.fullFilename, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0644); err != nil {
		r.Logger.Fatal().Err(err).Msgf("Fail to create file %s", data.fullFilename)
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
		for _, server := range r.rafty.options.Peers {
			r.rafty.configuration.ServerMembers = append(r.rafty.configuration.ServerMembers, peer{Address: server.Address})
		}
		r.rafty.mu.Unlock()
		return nil
	}

	_, _ = r.file.Seek(0, 0)
	result, err := io.ReadAll(r.file)
	if err != nil {
		return err
	}

	if len(result) > 0 {
		var data metadata
		err = json.Unmarshal(result, &data)
		if err != nil {
			return err
		}

		r.rafty.mu.Lock()
		r.rafty.id = data.Id
		r.rafty.currentTerm.Store(data.CurrentTerm)
		r.rafty.votedFor = data.VotedFor
		r.rafty.lastApplied.Store(data.LastApplied)
		r.rafty.lastAppliedConfig.Store(data.LastAppliedConfig)
		if len(data.Configuration.ServerMembers) > 0 {
			r.rafty.configuration.ServerMembers = data.Configuration.ServerMembers
		} else {
			for _, server := range r.rafty.options.Peers {
				r.rafty.configuration.ServerMembers = append(r.rafty.configuration.ServerMembers, peer{Address: server.Address})
			}
		}

		for i := range data.Configuration.ServerMembers {
			index := slices.IndexFunc(r.rafty.options.Peers, func(p Peer) bool {
				return p.address.String() == data.Configuration.ServerMembers[i].Address
			})
			if index == -1 {
				r.rafty.configuration.newMembers = append(r.rafty.configuration.newMembers, peer{Address: data.Configuration.ServerMembers[i].Address, ID: data.Configuration.ServerMembers[i].ID})
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
	if r.file == nil {
		return nil
	}

	r.rafty.mu.Lock()
	peers := r.rafty.configuration.ServerMembers
	r.rafty.mu.Unlock()

	data := metadata{
		Id:                r.rafty.id,
		CurrentTerm:       r.rafty.currentTerm.Load(),
		VotedFor:          r.rafty.votedFor,
		LastApplied:       r.rafty.lastApplied.Load(),
		LastAppliedConfig: r.rafty.lastAppliedConfig.Load(),
		Configuration: configuration{
			ServerMembers: peers,
		},
	}

	result, err := json.Marshal(data)
	if err != nil {
		return err
	}

	_ = r.file.Truncate(0)
	_, _ = r.file.Seek(0, 0)
	_, err = r.file.Write(result)
	if err != nil {
		return err
	}

	err = r.file.Sync()
	if err != nil {
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

	_, err := r.file.Seek(0, 0)
	if err != nil {
		return err
	}

	scanner := bufio.NewScanner(r.file)
	for scanner.Scan() {
		if len(scanner.Bytes()) > 0 {
			data, err := unmarshalBinary(scanner.Bytes())
			if err != nil && err != io.EOF {
				return err
			}
			r.rafty.logs.appendEntries([]*raftypb.LogEntry{data}, true)
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
	buffer := new(bytes.Buffer)
	err = marshalBinary(logEntry, buffer)
	if err != nil {
		return err
	}
	writer := bufio.NewWriter(r.file)

	if _, err = writer.Write(buffer.Bytes()); err != nil {
		return err
	}

	// Write a newline after each struct
	if _, err = writer.WriteString("\n"); err != nil {
		return err
	}

	if err = writer.Flush(); err != nil {
		return err
	}
	return nil
}

// store allow to persist data logs on disk
func (r dataFile) storeWithEntryIndex(entryIndex int) error {
	if r.file == nil {
		return nil
	}

	entry := r.rafty.logs.fromIndex(uint64(entryIndex)).logs[0]
	logEntry := &logEntry{
		FileFormat: uint8(entry.FileFormat),
		Tombstone:  uint8(entry.Tombstone),
		Timestamp:  entry.Timestamp,
		Term:       entry.Term,
		Command:    entry.Command,
	}

	var err error
	buffer := new(bytes.Buffer)
	err = marshalBinary(logEntry, buffer)
	if err != nil {
		return err
	}
	writer := bufio.NewWriter(r.file)

	if _, err = writer.Write(buffer.Bytes()); err != nil {
		return err
	}

	// Write a newline after each struct
	if _, err = writer.WriteString("\n"); err != nil {
		return err
	}

	if err = writer.Flush(); err != nil {
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
