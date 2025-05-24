package rafty

import (
	"math/rand"
	"time"
)

// randomElectionTimeout permit to generate a random value
// that will be used during the election campain
// when preVote set to true, a value will be generated for preVote election
func (r *Rafty) randomElectionTimeout() time.Duration {
	rd := rand.New(rand.NewSource(time.Now().UnixNano()))
	electionTimeoutMin := r.options.ElectionTimeout / 2
	return time.Duration(electionTimeoutMin+rd.Intn(r.options.ElectionTimeout-electionTimeoutMin)) * time.Millisecond * time.Duration(r.options.TimeMultiplier)
}

// heartbeatTimeout return leader heartbeat timeout
func (r *Rafty) heartbeatTimeout() time.Duration {
	return time.Duration(r.options.HeartbeatTimeout*int(r.options.TimeMultiplier)) * time.Millisecond
}

// randomRPCTimeout permit to generate a random value
// that will be used during RPC calls
func (r *Rafty) randomRPCTimeout(leader bool) time.Duration {
	var min, max int
	if leader {
		min, max = r.options.HeartbeatTimeout/4, r.options.HeartbeatTimeout/2
	} else {
		min, max = r.options.ElectionTimeout/4, r.options.ElectionTimeout/2
	}
	rd := rand.New(rand.NewSource(time.Now().UnixNano()))
	return time.Duration(min+rd.Intn(max-min)) * time.Millisecond * time.Duration(r.options.TimeMultiplier)
}
