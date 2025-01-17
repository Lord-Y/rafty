package rafty

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRandomElectionTimeout(t *testing.T) {
	assert := assert.New(t)

	s := basicNodeSetup()
	assert.NotNil(s.randomElectionTimeout())
}

func TestResetAndStopElectionTimer(t *testing.T) {
	assert := assert.New(t)

	s := basicNodeSetup()
	assert.Nil(s.electionTimer)
	s.electionTimer = time.NewTimer(s.randomElectionTimeout())
	assert.NotNil(s.electionTimer)
	s.resetElectionTimer()
	s.stopElectionTimer()
}
