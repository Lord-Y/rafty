package rafty

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestStateReadOnly(t *testing.T) {
	assert := assert.New(t)
	s := basicNodeSetup()
	err := s.parsePeers()
	assert.Nil(err)

	state := readOnly{rafty: s}

	s.timer = time.NewTicker(s.randomElectionTimeout())
	state.init()
	state.onTimeout()
	state.release()
}
