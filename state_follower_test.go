package rafty

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestStateFollower(t *testing.T) {
	assert := assert.New(t)

	s := basicNodeSetup()
	defer func() {
		assert.Nil(s.logStore.Close())
		assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
	}()
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
		s.State = Candidate
		state.onTimeout()
	})

	t.Run("onTimeout_minimum_cluster_size_not_reached", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.isRunning.Store(true)
		s.State = Follower
		state := follower{rafty: s}
		t.Run("init", func(t *testing.T) {
			state.init()
		})

		s.State = Follower
		s.minimumClusterSizeReach.Store(false)
		s.askForMembershipInProgress.Store(false)
		s.askForMembership.Store(false)
		s.decommissioning.Store(false)
		s.setLeader(leaderMap{})

		state.onTimeout()

		assert.Equal(Follower, s.getState())
		assert.False(s.minimumClusterSizeReach.Load())
	})

	t.Run("release", func(t *testing.T) {
		s.State = Follower
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
}
