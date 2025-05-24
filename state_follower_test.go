package rafty

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStateFollower_down(t *testing.T) {
	assert := assert.New(t)
	s := basicNodeSetup()
	err := s.parsePeers()
	assert.Nil(err)

	state := follower{rafty: s}
	state.onTimeout()
}
