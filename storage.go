package rafty

import (
	"bufio"
	"encoding/json"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

var (
	metadataFile  string = "metadata.json"
	dataStateDir  string = "wal"
	dataStateFile string = "state.bin"
)

// persistMetadata is a struct holding all requirements
// to persist node metadata
type persistMetadata struct {
	// Id of the current raft server
	Id string `json:"id"`

	// CurrentTerm is latest term seen during the voting campain
	CurrentTerm uint64 `json:"currentTerm"`

	// votedFor is the node the current node voted for during the election campain
	VotedFor string `json:"votedFor"`

	// Configuration hold server members
	Configuration configuration `json:"configuration"`
}

// restoreMetadata allow us to restore node metadata from disk
func (r *Rafty) restoreMetadata() {
	if !r.options.PersistDataOnDisk {
		r.mu.Lock()
		for _, server := range r.options.Peers {
			r.configuration.ServerMembers = append(r.configuration.ServerMembers, peer{Address: server.Address})
		}
		r.mu.Unlock()
		return
	}
	if r.options.DataDir == "" {
		return
	}

	err := createDirectoryIfNotExist(r.options.DataDir, 0750)
	if err != nil {
		r.Logger.Fatal().Err(err).Msgf("Fail to create directory %s", r.options.DataDir)
	}

	dataFile := filepath.Join(r.options.DataDir, metadataFile)
	if r.metadataFileDescriptor == nil {
		var err error
		r.metadataFileDescriptor, err = os.OpenFile(dataFile, os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			r.Logger.Fatal().Err(err).Msgf("Fail to create file %s", dataFile)
		}
	}

	result, err := io.ReadAll(r.metadataFileDescriptor)
	if err != nil {
		r.Logger.Fatal().Err(err).Msgf("Fail to read metadata file %s", dataFile)
	}

	if len(result) > 0 {
		var data persistMetadata
		err = json.Unmarshal(result, &data)
		// bypass invalid character '\\x00' looking for beginning of value as the file is in binary format
		if err != nil && !strings.Contains(err.Error(), "looking for beginning of value") {
			r.Logger.Fatal().Err(err).Msgf("Fail to unmarshall metadata file")
		}

		r.mu.Lock()
		r.id = data.Id
		r.CurrentTerm = data.CurrentTerm
		r.votedFor = data.VotedFor
		if len(data.Configuration.ServerMembers) > 0 {
			r.configuration.ServerMembers = data.Configuration.ServerMembers
		} else {
			for _, server := range r.options.Peers {
				r.configuration.ServerMembers = append(r.configuration.ServerMembers, peer{Address: server.Address})
			}
		}

		for i := range data.Configuration.ServerMembers {
			index := slices.IndexFunc(r.options.Peers, func(p Peer) bool {
				return p.address.String() == data.Configuration.ServerMembers[i].Address
			})
			if index == -1 {
				r.configuration.newMembers = append(r.configuration.newMembers, peer{Address: data.Configuration.ServerMembers[i].Address, ID: data.Configuration.ServerMembers[i].ID})
			}
		}
		r.mu.Unlock()
		return
	}

	r.mu.Lock()
	for _, server := range r.options.Peers {
		r.configuration.ServerMembers = append(r.configuration.ServerMembers, peer{Address: server.Address})
	}
	r.mu.Unlock()
}

// persistMetadata allow us to persist node metadata on disk
func (r *Rafty) persistMetadata() error {
	if !r.options.PersistDataOnDisk {
		return nil
	}
	if r.options.DataDir == "" {
		return nil
	}

	err := createDirectoryIfNotExist(r.options.DataDir, 0755)
	if err != nil {
		return err
	}

	_, myId := r.getMyAddress()
	votedFor, _ := r.getVotedFor()

	r.mu.Lock()
	peers := r.configuration.ServerMembers
	r.mu.Unlock()

	data := persistMetadata{
		Id:          myId,
		CurrentTerm: r.getCurrentTerm(),
		VotedFor:    votedFor,
		Configuration: configuration{
			ServerMembers: peers,
		},
	}

	result, err := json.Marshal(data)
	if err != nil {
		return err
	}

	dataFile := filepath.Join(r.options.DataDir, metadataFile)
	if r.metadataFileDescriptor == nil {
		var err error
		r.metadataFileDescriptor, err = os.OpenFile(dataFile, os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			r.Logger.Fatal().Err(err).Msgf("Fail to create file %s", dataFile)
		}
	}

	_ = r.metadataFileDescriptor.Truncate(0)
	_, err = r.metadataFileDescriptor.Write(result)
	if err != nil {
		return err
	}

	err = r.metadataFileDescriptor.Sync()
	if err != nil {
		return err
	}
	return nil
}

// restoreData allow us to restore node data from disk
func (r *Rafty) restoreData() {
	if !r.options.PersistDataOnDisk {
		return
	}
	if r.options.DataDir == "" {
		return
	}

	datadir := filepath.Join(r.options.DataDir, dataStateDir)
	err := createDirectoryIfNotExist(datadir, 0750)
	if err != nil {
		r.Logger.Fatal().Err(err).Msgf("Fail to create directory %s", datadir)
	}

	dataFile := filepath.Join(datadir, dataStateFile)
	if r.dataFileDescriptor == nil {
		var err error
		r.dataFileDescriptor, err = os.OpenFile(dataFile, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0644)
		if err != nil {
			r.Logger.Fatal().Err(err).Msgf("Fail to create file %s", dataFile)
		}
	}

	_, err = r.dataFileDescriptor.Seek(0, 0)
	if err != nil {
		r.Logger.Fatal().Err(err).Msgf("Fail to seek data in file %s", dataFile)
	}
	scanner := bufio.NewScanner(r.dataFileDescriptor)
	r.mu.Lock()
	for scanner.Scan() {
		if len(scanner.Bytes()) > 0 {
			data := r.unmarshalBinary(scanner.Bytes())
			r.log = append(r.log, data)
		}
	}
	r.mu.Unlock()
}

type logEntry struct {
	FileFormat uint8  // 1 byte
	Tombstone  uint8  // 1 byte
	Timestamp  uint32 // 4 bytes
	Term       uint64 // 4 bytes
	Command    []byte
}

// persistData allow us to persist node data on disk
func (r *Rafty) persistData(entryIndex int) error {
	if !r.options.PersistDataOnDisk {
		return nil
	}
	if r.options.DataDir == "" {
		return nil
	}

	datadir := filepath.Join(r.options.DataDir, dataStateDir)
	err := createDirectoryIfNotExist(datadir, 0750)
	if err != nil {
		r.Logger.Fatal().Err(err).Msgf("Fail to create directory %s", datadir)
	}

	dataFile := filepath.Join(datadir, dataStateFile)
	r.Logger.Debug().Msgf("dataFile %s", dataFile)
	if r.dataFileDescriptor == nil {
		var err error
		r.dataFileDescriptor, err = os.OpenFile(dataFile, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0644)
		if err != nil {
			r.Logger.Fatal().Err(err).Msgf("Fail to create file %s", dataFile)
		}
	}

	r.mu.Lock()
	entry := r.log[entryIndex]
	r.mu.Unlock()

	logEntry := &logEntry{
		FileFormat: uint8(entry.FileFormat),
		Tombstone:  uint8(entry.Tombstone),
		Timestamp:  entry.Timestamp,
		Term:       entry.Term,
		Command:    entry.Command,
	}

	data := r.marshalBinary(logEntry)
	writer := bufio.NewWriter(r.dataFileDescriptor)
	defer writer.Flush()

	_, err = writer.Write(data)
	if err != nil {
		return err
	}
	_, err = writer.WriteString("\n")
	if err != nil {
		return err
	}
	return nil
}

// createDirectoryIfNotExist permits to check if a directory exist
// and create it if not. An error will be return if there is any
func createDirectoryIfNotExist(d string, perm fs.FileMode) error {
	if _, err := os.Stat(d); os.IsNotExist(err) {
		err := os.MkdirAll(d, perm)
		if err != nil {
			return err
		}
		return nil
	}
	return nil
}

// closeAllFilesDescriptor allow us to close r.metadataFileDescriptor and r.dataFileDescriptor
func (r *Rafty) closeAllFilesDescriptor() {
	if r.metadataFileDescriptor != nil {
		r.metadataFileDescriptor.Close()
	}
	if r.dataFileDescriptor != nil {
		r.dataFileDescriptor.Close()
	}
}
