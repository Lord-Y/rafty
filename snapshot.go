package rafty

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"time"
)

var (
	snapshotMetadataFile string = "metadata.json"
	snapshotDir          string = "snapshots"
	snapshotStateFile    string = "snapshot.bin"
	snapshotTmpSuffix    string = ".tmp"
)

type SnapshotConfig struct {
	// parentDir is the directory that contain all snapshots
	parentDir string

	// dataDir is the directory in which the snapshot will be stored
	dataDir string

	// tmpDir is the termporary directory in which the snapshot will be stored
	tmpDir string

	// maxSnapshots is the max snapshots to keep
	maxSnapshots int
}

type SnapshotMetadata struct {
	// LastIncludedIndex is the last index included in the snapshot
	LastIncludedIndex uint64 `json:"lastIncludedIndex"`

	// LastIncludedTerm is the term of LastIncludedIndex
	LastIncludedTerm uint64 `json:"lastIncludedTerm"`

	// Configuration hold server members
	Configuration configuration `json:"configuration"`

	// LastAppliedConfig is the index of the highest log entry configuration applied to the current raft server
	LastAppliedConfigIndex uint64 `json:"lastAppliedConfigIndex"`

	// LastAppliedConfigTerm is the term of the highest log entry configuration applied to the current raft server
	LastAppliedConfigTerm uint64 `json:"lastAppliedConfigTerm"`

	// SnapshotName is the snapshot name
	SnapshotName string `json:"snapshotName"`

	// Size is the snapshot size
	Size int64

	// file is used to write metatadata to disk
	file *os.File
}

type SnapshotManager struct {
	io.ReadWriteSeeker

	// config is the snapshot config
	config *SnapshotConfig

	// metadata is the snapshot metadata
	metadata *SnapshotMetadata

	// file is used to write the snapshot to disk
	file *os.File

	// buffer will be used to write snapshot content
	buffer *bufio.Writer
}

type Snapshot interface {
	io.ReadWriteSeeker
	io.Closer

	// Name return the snapshot name
	Name() string

	// Metadata will return snapshot metadata
	Metadata() SnapshotMetadata

	// Discard will remove the snapshot actually in progress
	Discard() error

	// Reader will return snapshot metadata and data
	Reader() (bytes.Buffer, error)
}

// NewSnapshot will return the snapshot config that allow us to
// manage snapshots
func NewSnapshot(dataDir string, maxSnapshots int) *SnapshotConfig {
	if maxSnapshots == 0 {
		maxSnapshots = 1
	}

	parentDir := filepath.Join(dataDir, snapshotDir)
	return &SnapshotConfig{parentDir: parentDir, maxSnapshots: maxSnapshots}
}

// makeSnapshotName will return the snapshot name based on provided parameters
func makeSnapshotName(lastIncludedIndex, lastIncludedTerm uint64) string {
	now := time.Now()
	// index-term-timestamp
	return fmt.Sprintf("%d-%d-%d", lastIncludedIndex, lastIncludedTerm, now.UnixMilli())
}

// PrepareSnapshotWriter will prepare the requirements with the provided parameters to write a snapshot
func (s *SnapshotConfig) PrepareSnapshotWriter(lastIncludedIndex, lastIncludedTerm, lastAppliedConfigIndex, lastAppliedConfigTerm uint64, currentConfig configuration) (Snapshot, error) {
	snapshotName := makeSnapshotName(lastIncludedIndex, lastIncludedTerm)
	s.dataDir = filepath.Join(s.parentDir, snapshotName)
	s.tmpDir = filepath.Join(s.parentDir, snapshotName+snapshotTmpSuffix)
	if err := createDirectoryIfNotExist(s.tmpDir, 0750); err != nil {
		return nil, fmt.Errorf("fail to create directory %s: %w", s.tmpDir, err)
	}

	metadata := &SnapshotMetadata{
		LastIncludedIndex:      lastIncludedIndex,
		LastIncludedTerm:       lastIncludedTerm,
		LastAppliedConfigIndex: lastAppliedConfigTerm,
		SnapshotName:           snapshotName,
	}

	metadataFile := filepath.Join(s.tmpDir, snapshotMetadataFile)
	file, err := os.OpenFile(metadataFile, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("fail to create snapshot metadata file %s: %w", metadataFile, err)
	}
	metadata.file = file

	snapshotFile := filepath.Join(s.tmpDir, snapshotStateFile)
	dataFile, err := os.OpenFile(snapshotFile, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("fail to create snapshot file %s: %w", snapshotFile, err)
	}

	snapshotManager := &SnapshotManager{
		ReadWriteSeeker: dataFile,
		config:          s,
		metadata:        metadata,
		file:            dataFile,
		buffer:          bufio.NewWriter(dataFile),
	}
	return snapshotManager, nil
}

// PrepareSnapshotReader will return the appropriate config allowing to read
// the snapshot name
func (s *SnapshotConfig) PrepareSnapshotReader(name string) (Snapshot, error) {
	s.dataDir = filepath.Join(s.parentDir, name)
	metadataFilePath := filepath.Join(s.dataDir, snapshotMetadataFile)
	metadataFile, err := os.Open(metadataFilePath)
	if err != nil {
		return nil, err
	}

	metadata, err := s.readMetadata(metadataFile)
	if err != nil {
		return nil, err
	}

	dataFilePath := filepath.Join(s.dataDir, snapshotStateFile)
	dataFile, err := os.Open(dataFilePath)
	if err != nil {
		return nil, err
	}

	snapshotManager := &SnapshotManager{
		ReadWriteSeeker: dataFile,
		config:          s,
		metadata:        metadata,
		file:            dataFile,
		buffer:          bufio.NewWriter(dataFile),
	}
	return snapshotManager, nil
}

// List will return the list of snapshots
func (s *SnapshotConfig) List() (l []*SnapshotMetadata) {
	dirs, err := os.ReadDir(s.parentDir)
	if err != nil {
		return nil
	}
	for _, dir := range dirs {
		snapshotFile := filepath.Join(s.parentDir, dir.Name(), snapshotMetadataFile)
		data, err := os.ReadFile(snapshotFile)
		if err != nil {
			return nil
		}
		z := &SnapshotMetadata{}
		if err := json.Unmarshal(data, z); err != nil {
			return nil
		}
		l = append(l, z)
	}
	return
}

// Name will return the snapshot name
func (s *SnapshotManager) Name() string {
	return s.metadata.SnapshotName
}

// Metadata will return snapshot metadata
func (s *SnapshotManager) Metadata() SnapshotMetadata {
	return *s.metadata
}

// Reader will return snapshot metadata and data
// The underlying files handlers will autocatically closed
func (s *SnapshotManager) Reader() (bytes.Buffer, error) {
	var buf bytes.Buffer
	_, err := io.Copy(&buf, s.file)
	_ = s.file.Close()
	_ = s.metadata.file.Close()
	return buf, err
}

// Close will close the snapshot file
func (s *SnapshotManager) Close() error {
	if s.file == nil {
		return nil
	}

	// Sync() force the OS to flush its cache to disk guaranteeing the data
	// is physically written to disk.
	if err := s.file.Sync(); err != nil {
		return err
	}

	stat, err := s.file.Stat()
	if err != nil {
		return err
	}
	s.metadata.Size = stat.Size()

	if err := s.file.Close(); err != nil {
		return err
	}

	if err := s.writeMetadata(); err != nil {
		return err
	}

	// rename snapshot tempory dir to final directory
	if err := os.Rename(s.config.tmpDir, s.config.dataDir); err != nil {
		return err
	}
	return s.removeOldSnapshots()
}

// Discard will remove the snapshot actually in progress
func (s *SnapshotManager) Discard() error {
	if s.file == nil {
		return nil
	}

	if err := s.file.Close(); err != nil {
		return err
	}
	s.file = nil
	s.metadata.file = nil

	if err := os.RemoveAll(s.config.tmpDir); err != nil {
		return err
	}
	return nil
}

// writeMetadata will encode and write snapshot metadata
func (s *SnapshotManager) writeMetadata() error {
	result, err := json.Marshal(s.metadata)
	if err != nil {
		return err
	}

	if _, err := s.metadata.file.Write(result); err != nil {
		return err
	}

	if err = s.metadata.file.Sync(); err != nil {
		return fmt.Errorf("fail to sync snapshot metadata file %s: %w", snapshotMetadataFile, err)
	}
	if err = s.metadata.file.Close(); err != nil {
		return fmt.Errorf("fail to close snapshot metadata file %s: %w", snapshotMetadataFile, err)
	}
	return nil
}

// readMetadata will read snapshot metadata
func (s *SnapshotConfig) readMetadata(reader io.Reader) (*SnapshotMetadata, error) {
	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("fail to read metadata file %w", err)
	}
	metadata := &SnapshotMetadata{}
	if err := json.Unmarshal(data, metadata); err != nil {
		return nil, fmt.Errorf("fail to unmarshal metadata file %w", err)
	}
	return metadata, nil
}

// removeOldSnapshots will remove old snapshots from the parent directory
func (s *SnapshotManager) removeOldSnapshots() error {
	dirs, err := os.ReadDir(s.config.parentDir)
	if err != nil {
		return err
	}
	sort.Slice(dirs, func(i, j int) bool {
		var index1, term1, timestamp1 int64
		var index2, term2, timestamp2 int64
		pattern := "%d-%d-%d"
		_, _ = fmt.Sscanf(dirs[i].Name(), pattern, &index1, &term1, timestamp1)
		_, _ = fmt.Sscanf(dirs[j].Name(), pattern, &index2, &term2, timestamp2)

		switch index1 {
		case index2:
			if term1 == term2 {
				return timestamp1 > timestamp2
			}
			return term1 > term2
		default:
			return index1 > index2
		}
	})

	for i, dir := range dirs {
		if dir.IsDir() && i >= s.config.maxSnapshots {
			_ = os.RemoveAll(filepath.Join(s.config.parentDir, dir.Name()))
		}
	}
	return nil
}
