package rafty

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStateDown(t *testing.T) {
	assert := assert.New(t)
	s := basicNodeSetup()
	err := s.parsePeers()
	assert.Nil(err)

	state := down{rafty: s}

	state.init()
	state.onTimeout()
	state.release()
}
