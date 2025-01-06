package rafty

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRandomElectionTimeout(t *testing.T) {
	assert := assert.New(t)

	s := basicNodeSetup()
	assert.NotNil(s.randomElectionTimeout(true))
	assert.NotNil(s.randomElectionTimeout(false))
}

func TestResetAndStopElectionTimer(t *testing.T) {
	assert := assert.New(t)

	s := basicNodeSetup()
	assert.Nil(s.preVoteElectionTimer)
	assert.Nil(s.electionTimer)
	s.preVoteElectionTimer = time.NewTimer(s.randomElectionTimeout(true))
	s.electionTimer = time.NewTimer(s.randomElectionTimeout(false))
	assert.NotNil(s.preVoteElectionTimer)
	assert.NotNil(s.electionTimer)
	s.resetElectionTimer(true, true)
	s.stopElectionTimer(true, true)
}
