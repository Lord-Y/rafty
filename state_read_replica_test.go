package rafty

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestStateReadReplica(t *testing.T) {
	assert := assert.New(t)
	s := basicNodeSetup()
	err := s.parsePeers()
	assert.Nil(err)

	s.isRunning.Store(true)
	s.State = ReadReplica
	state := readReplica{rafty: s}

	t.Run("init", func(t *testing.T) {
		state.init()
	})

	t.Run("onTimeout", func(t *testing.T) {
		state.onTimeout()
	})

	t.Run("release", func(t *testing.T) {
		state.release()
	})

	t.Run("membership_timeout", func(t *testing.T) {
		state.rafty.askForMembership.Store(true)
		state.onTimeout()
	})

	t.Run("membership_in_progress", func(t *testing.T) {
		state.rafty.askForMembershipInProgress.Store(true)
		defer state.rafty.askForMembershipInProgress.Store(true)
		state.onTimeout()
	})

	t.Run("membership_stop", func(t *testing.T) {
		s.quitCtx, s.stopCtx = context.WithTimeout(context.Background(), time.Second)
		defer s.stopCtx()
		state.askForMembership()
	})
}
