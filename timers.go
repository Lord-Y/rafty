package rafty

import (
	"math/rand"
	"sync"
	"time"
)

var (
	randomTimeoutMu  sync.Mutex
	randomTimeoutRNG = rand.New(rand.NewSource(time.Now().UnixNano()))
)

// electionTimeout return election timeout based on time multiplier
func (r *Rafty) electionTimeout() time.Duration {
	return time.Duration(r.options.ElectionTimeout) * time.Millisecond * time.Duration(r.options.TimeMultiplier)
}

// randomElectionTimeout permit to generate a random value
// that will be used during the election campaign
// when preVote set to true, a value will be generated for preVote election
func (r *Rafty) randomElectionTimeout() time.Duration {
	electionTimeoutMin := r.options.ElectionTimeout / 2
	return time.Duration(electionTimeoutMin+r.randIntn(r.options.ElectionTimeout-electionTimeoutMin)) * time.Millisecond * time.Duration(r.options.TimeMultiplier)
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
		min, max = r.options.HeartbeatTimeout/3, r.options.HeartbeatTimeout/2
	} else {
		min, max = r.options.ElectionTimeout/3, r.options.ElectionTimeout/2
	}
	return time.Duration(min+r.randIntn(max-min)) * time.Millisecond * time.Duration(r.options.TimeMultiplier)
}

// randomTimeout will return a random timeout base on the
// duration provided
func randomTimeout(duration time.Duration) time.Duration {
	randomTimeoutMu.Lock()
	defer randomTimeoutMu.Unlock()
	return duration + time.Duration(randomTimeoutRNG.Int63())%duration
}

// randIntn returns a pseudo-random int in [0,max) using Rafty's shared RNG.
func (r *Rafty) randIntn(max int) int {
	if max <= 0 {
		return 0
	}
	r.randMu.Lock()
	defer r.randMu.Unlock()
	return r.rand.Intn(max)
}
