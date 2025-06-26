package rafty

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestStateFollower(t *testing.T) {
	assert := assert.New(t)
	s := basicNodeSetup()
	err := s.parsePeers()
	assert.Nil(err)

	s.isRunning.Store(true)
	s.State = Follower
	state := follower{rafty: s}
	t.Run("init", func(t *testing.T) {
		state.init()
	})

	t.Run("onTimeout_leader_lost", func(t *testing.T) {
		s.leaderLastContactDate.Store(time.Now())
		time.Sleep(2 * time.Second)
		state.onTimeout()
	})

	t.Run("onTimeout_not_follower", func(t *testing.T) {
		s.State = ReadOnly
		state.onTimeout()
	})

	t.Run("release", func(t *testing.T) {
		state.release()
	})
}
