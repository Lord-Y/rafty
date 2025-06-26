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

	s.isRunning.Store(true)
	s.State = ReadOnly
	state := readOnly{rafty: s}

	t.Run("init", func(t *testing.T) {
		state.init()
	})

	t.Run("onTimeout", func(t *testing.T) {
		state.onTimeout()
	})

	t.Run("release", func(t *testing.T) {
		state.release()
	})
}
