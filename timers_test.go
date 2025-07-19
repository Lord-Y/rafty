package rafty

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestElectionTimeout(t *testing.T) {
	assert := assert.New(t)

	s := basicNodeSetup()
	assert.NotNil(s.electionTimeout())
}

func TestRandomElectionTimeout(t *testing.T) {
	assert := assert.New(t)

	s := basicNodeSetup()
	assert.NotNil(s.randomElectionTimeout())
}

func TestHeartbeatTimeout(t *testing.T) {
	assert := assert.New(t)

	s := basicNodeSetup()
	assert.NotNil(s.heartbeatTimeout())
}

func TestRandomRPCTimeout(t *testing.T) {
	assert := assert.New(t)

	s := basicNodeSetup()
	assert.NotNil(s.randomRPCTimeout(true))
	assert.NotNil(s.randomRPCTimeout(false))
}
