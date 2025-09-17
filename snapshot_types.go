package rafty

import (
	"bufio"
	"io"
	"os"
)

var (
	snapshotMetadataFile string = "metadata.json"
	snapshotDir          string = "snapshots"
	snapshotStateFile    string = "snapshot.bin"
	snapshotTmpSuffix    string = ".tmp"
)

// SnapshotConfig return the config that will be use
// to manipulate snapshots
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

// SnapshotMetadata hold the snapshot metadata
type SnapshotMetadata struct {
	// LastIncludedIndex is the last index included in the snapshot
	LastIncludedIndex uint64 `json:"lastIncludedIndex"`

	// LastIncludedTerm is the term of LastIncludedIndex
	LastIncludedTerm uint64 `json:"lastIncludedTerm"`

	// Configuration hold server members
	Configuration Configuration `json:"configuration"`

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

// SnapshotStore is an interface that allow end user to
// read or take snapshots
type SnapshotStore interface {
	// PrepareSnapshotWriter will prepare the requirements with the provided parameters to write a snapshot
	PrepareSnapshotWriter(lastIncludedIndex, lastIncludedTerm, lastAppliedConfigIndex, lastAppliedConfigTerm uint64, currentConfig Configuration) (Snapshot, error)

	// PrepareSnapshotReader will return the appropriate config to read
	// the snapshot name
	PrepareSnapshotReader(name string) (Snapshot, io.ReadCloser, error)

	// List will return the list of snapshots
	List() []*SnapshotMetadata
}

// SnapshotManager allow us to manage snapshots
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

// Snapshot is that implements SnapshotManager
type Snapshot interface {
	io.ReadWriteSeeker
	io.Closer

	// Name return the snapshot name
	Name() string

	// Metadata will return snapshot metadata
	Metadata() SnapshotMetadata

	// Discard will remove the snapshot actually in progress
	Discard() error
}
