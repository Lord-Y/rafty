package rafty

import "sync"

// candidate holds all requirements by a node in candidate state
type candidate struct {
	// rafty holds rafty config
	rafty *Rafty

	// mu is used to ensure lock concurrency
	mu sync.Mutex

	// responsePreVoteChan is use to manage pre vote responses
	responsePreVoteChan chan RPCResponse

	// responseVoteChan is use to manage vote responses
	responseVoteChan chan RPCResponse

	// preCandidatePeers holds the list of the peers that will be used
	// during election campaign to elect a new leader
	preCandidatePeers []Peer

	// preVotes is the total number of votes used to be compare against quorum
	preVotes int

	// votes is the total number of votes used to be compare against quorum
	votes int

	// quorum is the minimum number of votes to reach before starting election
	// or becoming the new leader
	quorum int
}
