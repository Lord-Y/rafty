package rafty

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTimers_electionTimeout(t *testing.T) {
	assert := assert.New(t)

	s := basicNodeSetup()
	defer func() {
		assert.Nil(s.logStore.Close())
	}()
	assert.NotNil(s.electionTimeout())
}

func TestTimers_randomElectionTimeout(t *testing.T) {
	assert := assert.New(t)

	s := basicNodeSetup()
	defer func() {
		assert.Nil(s.logStore.Close())
	}()
	assert.NotNil(s.randomElectionTimeout())
}

func TestTimers_heartbeatTimeout(t *testing.T) {
	assert := assert.New(t)

	s := basicNodeSetup()
	defer func() {
		assert.Nil(s.logStore.Close())
	}()
	assert.NotNil(s.heartbeatTimeout())
}

func TestTimers_randomRPCTimeout(t *testing.T) {
	assert := assert.New(t)

	s := basicNodeSetup()
	defer func() {
		assert.Nil(s.logStore.Close())
	}()
	assert.NotNil(s.randomRPCTimeout(true))
	assert.NotNil(s.randomRPCTimeout(false))
}
