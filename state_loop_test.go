package rafty

import (
	"context"
	"testing"
	"time"

	"github.com/Lord-Y/rafty/raftypb"
	"github.com/stretchr/testify/assert"
)

func TestStateLoop_runAsReadReplica(t *testing.T) {
	assert := assert.New(t)
	s := basicNodeSetup()
	err := s.parsePeers()
	assert.Nil(err)

	s.quitCtx, s.stopCtx = context.WithTimeout(context.Background(), 2*time.Second)
	s.isRunning.Store(true)
	s.State = ReadReplica
	s.timer = time.NewTicker(s.randomElectionTimeout())

	t.Run("release", func(t *testing.T) {
		go s.runAsReadReplica()
		responseChan := make(chan RPCResponse, 1)
		s.rpcMembershipChangeRequestChan <- RPCRequest{
			RPCType:      MembershipChangeRequest,
			Request:      &raftypb.MembershipChangeRequest{Id: "newnode", Address: "127.0.0.1:60000", Action: uint32(Add)},
			ResponseChan: responseChan,
		}
		data := <-responseChan
		assert.Equal(ErrNotLeader, data.Error)
	})
}
