package rafty

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMembership_string(t *testing.T) {
	assert := assert.New(t)

	tests := []MembershipChange{
		Add,
		Promote,
		Demote,
		Remove,
		ForceRemove,
		LeaveOnTerminate,
	}
	results := []string{
		"add",
		"promote",
		"demote",
		"remove",
		"forceRemove",
		"leaveOnTerminate",
	}

	for k, v := range tests {
		assert.Equal(v.String() == results[k], true)
	}
}

func TestMembership_nextConfiguration(t *testing.T) {
	assert := assert.New(t)

	t.Run("add", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.fillIDs()
		s.State = Leader

		tests := []struct {
			member              Peer
			expWaitToBePromoted bool
			expDecommissioning  bool
			expError            error
			expContained        bool
			expVerify           bool
		}{
			{
				member:              Peer{ID: "newnode", IsVoter: true},
				expWaitToBePromoted: true,
				expContained:        true,
				expVerify:           true,
			},
			{
				member:              Peer{ID: "newnode"},
				expWaitToBePromoted: true,
				expContained:        true,
				expVerify:           true,
			},
		}

		for i, tc := range tests {
			peers, _ := s.getAllPeers()
			tc.member.ID = fmt.Sprintf("%s_%d", tc.member.ID, i)
			tc.member.Address = fmt.Sprintf("127.0.0.1:%d", 60000+i)
			next, err := s.nextConfiguration(Add, peers, tc.member)
			if err == nil {
				s.updateServerMembers(next)
			}
			assert.Equal(tc.expError, err)
			assert.Equal(tc.expWaitToBePromoted, next[len(next)-1].WaitToBePromoted)
			assert.Equal(tc.expDecommissioning, next[len(next)-1].Decommissioning)
			assert.Equal(tc.expContained, isPartOfTheCluster(next, tc.member))
			assert.Equal(tc.expVerify, s.verifyConfiguration(next))
		}
	})

	t.Run("remove", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.fillIDs()
		s.State = Leader

		tests := []struct {
			member              Peer
			action              MembershipChange
			expWaitToBePromoted bool
			expDecommissioning  bool
			expError            error
			expContained        bool
			expVerify           bool
		}{
			{
				member:              Peer{ID: "newnode", IsVoter: true},
				action:              Remove,
				expWaitToBePromoted: false,
				expContained:        false,
				expVerify:           true,
			},
			{
				member:              Peer{ID: "newnode"},
				action:              ForceRemove,
				expWaitToBePromoted: false,
				expContained:        false,
				expVerify:           true,
			},
		}

		for i, tc := range tests {
			peers, _ := s.getAllPeers()
			tc.member.ID = fmt.Sprintf("%s_%d", tc.member.ID, i)
			tc.member.Address = fmt.Sprintf("127.0.0.1:%d", 60000+i)
			tc.member.address = getNetAddress(tc.member.Address)
			next, err := s.nextConfiguration(Add, peers, tc.member)
			if err == nil {
				s.updateServerMembers(next)
			}
			assert.Equal(tc.expError, err)
			next, err = s.nextConfiguration(tc.action, next, tc.member)
			if err == nil {
				s.updateServerMembers(next)
			}
			assert.Equal(tc.expError, err)
			assert.Equal(tc.expWaitToBePromoted, next[len(next)-1].WaitToBePromoted)
			assert.Equal(tc.expDecommissioning, next[len(next)-1].Decommissioning)
			assert.Equal(tc.expContained, isPartOfTheCluster(next, tc.member))
			assert.Equal(tc.expVerify, s.verifyConfiguration(next))
		}
	})

	t.Run("promote", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.fillIDs()
		s.State = Leader

		tests := []struct {
			member              Peer
			expWaitToBePromoted bool
			expDecommissioning  bool
			expError            error
			expContained        bool
			expVerify           bool
		}{
			{
				member:              Peer{ID: "newnode", IsVoter: true},
				expWaitToBePromoted: false,
				expDecommissioning:  false,
				expContained:        true,
				expVerify:           true,
			},
			{
				member:              Peer{ID: "newnode"},
				expWaitToBePromoted: false,
				expDecommissioning:  false,
				expContained:        true,
				expVerify:           true,
			},
		}

		for i, tc := range tests {
			peers, _ := s.getAllPeers()
			tc.member.ID = fmt.Sprintf("%s_%d", tc.member.ID, i)
			tc.member.Address = fmt.Sprintf("127.0.0.1:%d", 60000+i)
			tc.member.address = getNetAddress(tc.member.Address)
			next, err := s.nextConfiguration(Add, peers, tc.member)
			if err == nil {
				s.updateServerMembers(next)
			}
			assert.Equal(tc.expError, err)
			next, err = s.nextConfiguration(Promote, next, tc.member)
			if err == nil {
				s.updateServerMembers(next)
			}

			assert.Equal(tc.expError, err)
			assert.Equal(tc.expWaitToBePromoted, next[len(next)-1].WaitToBePromoted)
			assert.Equal(tc.expDecommissioning, next[len(next)-1].Decommissioning)
			assert.Equal(tc.expContained, isPartOfTheCluster(next, tc.member))
			assert.Equal(tc.expVerify, s.verifyConfiguration(next))
		}
	})

	t.Run("demoted", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.fillIDs()
		s.State = Leader

		tests := []struct {
			member              Peer
			expWaitToBePromoted bool
			expDecommissioning  bool
			expError            error
			expContained        bool
			expVerify           bool
		}{
			{
				member:              Peer{ID: "newnode", IsVoter: true},
				expWaitToBePromoted: false,
				expDecommissioning:  true,
				expContained:        true,
				expVerify:           true,
			},
			{
				member:              Peer{ID: "newnode"},
				expWaitToBePromoted: false,
				expDecommissioning:  true,
				expContained:        true,
				expVerify:           true,
			},
		}

		for i, tc := range tests {
			peers, _ := s.getAllPeers()
			tc.member.ID = fmt.Sprintf("%s_%d", tc.member.ID, i)
			tc.member.Address = fmt.Sprintf("127.0.0.1:%d", 60000+i)
			tc.member.address = getNetAddress(tc.member.Address)
			next, err := s.nextConfiguration(Add, peers, tc.member)
			if err == nil {
				s.updateServerMembers(next)
			}
			assert.Equal(tc.expError, err)
			next, err = s.nextConfiguration(Promote, next, tc.member)
			if err == nil {
				s.updateServerMembers(next)
			}
			assert.Equal(tc.expError, err)
			next, err = s.nextConfiguration(Demote, next, tc.member)
			if err == nil {
				s.updateServerMembers(next)
			}

			assert.Equal(tc.expError, err)
			assert.Equal(tc.expWaitToBePromoted, next[len(next)-1].WaitToBePromoted)
			assert.Equal(tc.expDecommissioning, next[len(next)-1].Decommissioning)
			assert.Equal(tc.expContained, isPartOfTheCluster(next, tc.member))
			assert.Equal(tc.expVerify, s.verifyConfiguration(next))
		}
	})

	t.Run("all", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.fillIDs()
		s.State = Leader

		tests := []struct {
			member              Peer
			expWaitToBePromoted bool
			expDecommissioning  bool
			expError            error
			expContained        bool
			expVerify           bool
		}{
			{
				member:    Peer{ID: "newnode", IsVoter: true},
				expVerify: true,
			},
			{
				member:    Peer{ID: "newnode"},
				expVerify: true,
			},
		}

		for i, tc := range tests {
			peers, _ := s.getAllPeers()
			tc.member.ID = fmt.Sprintf("%s_%d", tc.member.ID, i)
			tc.member.Address = fmt.Sprintf("127.0.0.1:%d", 60000+i)
			tc.member.address = getNetAddress(tc.member.Address)

			next, err := s.nextConfiguration(Add, peers, tc.member)
			if err == nil {
				s.updateServerMembers(next)
			}
			assert.Equal(tc.expError, err)
			next, err = s.nextConfiguration(Promote, next, tc.member)
			if err == nil {
				s.updateServerMembers(next)
			}
			assert.Equal(tc.expError, err)
			next, err = s.nextConfiguration(Demote, next, tc.member)
			if err == nil {
				s.updateServerMembers(next)
			}
			assert.Equal(tc.expError, err)
			next, err = s.nextConfiguration(Remove, next, tc.member)
			if err == nil {
				s.updateServerMembers(next)
			}

			assert.Equal(tc.expError, err)
			assert.Equal(tc.expWaitToBePromoted, next[len(next)-1].WaitToBePromoted)
			assert.Equal(tc.expDecommissioning, next[len(next)-1].Decommissioning)
			assert.Equal(tc.expContained, isPartOfTheCluster(next, tc.member))
			assert.Equal(tc.expVerify, s.verifyConfiguration(next))
		}
	})

	t.Run("breaking", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.fillIDs()
		s.State = Leader

		tests := []struct {
			action              MembershipChange
			member              Peer
			memberid            int
			expWaitToBePromoted bool
			expDecommissioning  bool
			expError            error
			expContained        bool
			expVerify           bool
		}{
			{
				action:       Demote,
				memberid:     0,
				expContained: true,
				expVerify:    true,
			},
			{
				action:    Demote,
				memberid:  1,
				expVerify: false,
				expError:  ErrMembershipChangeNodeDemotionForbidden,
			},
		}

		for _, tc := range tests {
			peers, _ := s.getAllPeers()
			tc.member = peers[tc.memberid]
			next, err := s.nextConfiguration(tc.action, peers, tc.member)
			if err == nil {
				s.updateServerMembers(next)
				assert.Equal(tc.expError, err)
				assert.Equal(tc.expWaitToBePromoted, next[len(next)-1].WaitToBePromoted)
				assert.Equal(tc.expDecommissioning, next[len(next)-1].Decommissioning)
				assert.Equal(tc.expContained, isPartOfTheCluster(next, tc.member))
				assert.Equal(tc.expVerify, s.verifyConfiguration(next))
			}
		}
	})

	t.Run("leaveOnTerminate", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.fillIDs()
		s.State = Leader

		peers, _ := s.getAllPeers()
		tests := []struct {
			member              Peer
			expWaitToBePromoted bool
			expDecommissioning  bool
			expError            error
			expContained        bool
			expVerify           bool
		}{
			{
				member:    Peer{ID: "newnode", IsVoter: true},
				expVerify: true,
			},
			{
				member:    Peer{ID: "newnode"},
				expVerify: true,
			},
		}

		for i, tc := range tests {
			tc.member.ID = fmt.Sprintf("%s_%d", tc.member.ID, i)
			tc.member.Address = fmt.Sprintf("127.0.0.1:%d", 60000+i)
			tc.member.address = getNetAddress(tc.member.Address)
			next, err := s.nextConfiguration(Add, peers, tc.member)
			if err == nil {
				s.updateServerMembers(next)
			}
			assert.Equal(tc.expError, err)
			next, err = s.nextConfiguration(Promote, next, tc.member)
			if err == nil {
				s.updateServerMembers(next)
			}
			assert.Equal(tc.expError, err)
			next, err = s.nextConfiguration(LeaveOnTerminate, next, tc.member)
			if err == nil {
				s.updateServerMembers(next)
			}

			assert.Equal(tc.expError, err)
			assert.Equal(tc.expWaitToBePromoted, next[len(next)-1].WaitToBePromoted)
			assert.Equal(tc.expDecommissioning, next[len(next)-1].Decommissioning)
			assert.Equal(tc.expContained, isPartOfTheCluster(next, tc.member))
			assert.Equal(tc.expVerify, s.verifyConfiguration(next))
		}
	})
}

func TestMembership_changeRequest(t *testing.T) {
	assert := assert.New(t)

	member := Peer{
		ID:      "newbie",
		Address: "127.0.0.1:60000",
	}
	member.address = getNetAddress(member.Address)

	t.Run("add", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.fillIDs()
		s.State = Leader
		s.isRunning.Store(true)

		_, err := s.validateSendMembershipChangeRequest(Add, member)
		assert.Nil(err)
	})

	t.Run("promote", func(t *testing.T) {
		s := basicNodeSetup()
		assert.Nil(s.logStore.Close())
		defer func() {
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.fillIDs()
		s.State = Leader
		s.isRunning.Store(true)

		mmember := member
		mmember.WaitToBePromoted = true
		s.configuration.ServerMembers = append(s.configuration.ServerMembers, mmember)

		_, err := s.validateSendMembershipChangeRequest(Promote, mmember)
		assert.Nil(err)
	})

	t.Run("demote", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.fillIDs()
		s.State = Leader
		s.isRunning.Store(true)

		_, err := s.demoteNode(Demote, member)
		assert.Nil(err)

		s.configuration.ServerMembers[0].Decommissioning = true
		_, err = s.validateSendMembershipChangeRequest(Demote, s.configuration.ServerMembers[1])
		assert.Error(err)
		s.wg.Wait()
	})

	t.Run("demote_myself", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.fillIDs()
		s.State = Leader
		s.isRunning.Store(true)

		member := Peer{
			ID:      s.id,
			Address: s.Address.String(),
		}
		_, err := s.validateSendMembershipChangeRequest(Demote, member)
		assert.Nil(err)
		assert.Equal(false, s.isPartOfTheCluster(member))
	})

	t.Run("removeNode_force", func(t *testing.T) {
		s := basicNodeSetup()
		assert.Nil(s.logStore.Close())
		defer func() {
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.fillIDs()
		s.State = Leader
		s.isRunning.Store(true)

		_, err := s.validateSendMembershipChangeRequest(ForceRemove, member)
		assert.Nil(err)
	})

	t.Run("removeNode_return", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.fillIDs()
		s.State = Leader
		s.isRunning.Store(true)
		member := Peer{
			ID:              s.configuration.ServerMembers[0].ID,
			Address:         s.configuration.ServerMembers[0].Address,
			Decommissioning: true,
		}
		_, err := s.validateSendMembershipChangeRequest(Remove, member)
		assert.Error(err)
	})

	t.Run("removeNode_leaveOnTerminate", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.fillIDs()
		s.State = Leader
		s.isRunning.Store(true)
		member := Peer{
			ID:      s.configuration.ServerMembers[0].ID,
			Address: s.configuration.ServerMembers[0].Address,
		}

		_, err := s.validateSendMembershipChangeRequest(LeaveOnTerminate, member)
		assert.Nil(err)
	})

	t.Run("err_unknown", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.fillIDs()
		s.State = Leader
		s.isRunning.Store(true)
		member := Peer{
			ID:      s.configuration.ServerMembers[0].ID,
			Address: s.configuration.ServerMembers[0].Address,
		}

		_, err := s.validateSendMembershipChangeRequest(30, member)
		assert.ErrorIs(err, ErrUnkown)
	})
}
