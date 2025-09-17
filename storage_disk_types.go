package rafty

import "os"

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
	Configuration Configuration `json:"configuration"`

	// LastIncludedIndex is the index included in the last snapshot
	LastIncludedIndex uint64 `json:"lastIncludedIndex"`

	// lastIncludedTerm is the term linked to LastSnapshotIndex
	LastIncludedTerm uint64 `json:"lastIncludedTerm"`
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
