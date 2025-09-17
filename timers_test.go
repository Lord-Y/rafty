package rafty

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTimers_electionTimeout(t *testing.T) {
	assert := assert.New(t)

	s := basicNodeSetup()
	defer func() {
		assert.Nil(s.logStore.Close())
		assert.Nil(os.RemoveAll(s.options.DataDir))
	}()
	assert.NotNil(s.electionTimeout())
}

func TestTimers_randomElectionTimeout(t *testing.T) {
	assert := assert.New(t)

	s := basicNodeSetup()
	defer func() {
		assert.Nil(s.logStore.Close())
		assert.Nil(os.RemoveAll(s.options.DataDir))
	}()
	assert.NotNil(s.randomElectionTimeout())
}

func TestTimers_heartbeatTimeout(t *testing.T) {
	assert := assert.New(t)

	s := basicNodeSetup()
	defer func() {
		assert.Nil(s.logStore.Close())
		assert.Nil(os.RemoveAll(s.options.DataDir))
	}()
	assert.NotNil(s.heartbeatTimeout())
}

func TestTimers_randomRPCTimeout(t *testing.T) {
	assert := assert.New(t)

	s := basicNodeSetup()
	defer func() {
		assert.Nil(s.logStore.Close())
		assert.Nil(os.RemoveAll(s.options.DataDir))
	}()
	assert.NotNil(s.randomRPCTimeout(true))
	assert.NotNil(s.randomRPCTimeout(false))
}

func TestTimers_randomTimeout(t *testing.T) {
	assert := assert.New(t)

	x := time.Hour
	assert.Greater(randomTimeout(x), x)
}
