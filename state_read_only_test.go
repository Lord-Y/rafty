package rafty

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStateReadOnly(t *testing.T) {
	assert := assert.New(t)
	s := basicNodeSetup()
	err := s.parsePeers()
	assert.Nil(err)

	state := readOnly{rafty: s}

	state.init()
	state.onTimeout()
	state.release()
}
