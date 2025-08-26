package rafty

import (
	"math/rand"
	"time"
)

// electionTimeout return election timeout based on time multiplier
func (r *Rafty) electionTimeout() time.Duration {
	return time.Duration(r.options.ElectionTimeout) * time.Millisecond * time.Duration(r.options.TimeMultiplier)
}

// randomElectionTimeout permit to generate a random value
// that will be used during the election campaign
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
		min, max = r.options.HeartbeatTimeout/3, r.options.HeartbeatTimeout/2
	} else {
		min, max = r.options.ElectionTimeout/3, r.options.ElectionTimeout/2
	}
	rd := rand.New(rand.NewSource(time.Now().UnixNano()))
	return time.Duration(min+rd.Intn(max-min)) * time.Millisecond * time.Duration(r.options.TimeMultiplier)
}

// randomTimeout will return a random timeout base on the
// duration provided
func randomTimeout(duration time.Duration) time.Duration {
	return duration + time.Duration(rand.Int63())%duration
}
