package rafty

import (
	"testing"
	"time"

	"github.com/Lord-Y/rafty/raftypb"
	"github.com/stretchr/testify/assert"
)

func TestDrainPreVoteRequests(t *testing.T) {
	assert := assert.New(t)

	s := basicNodeSetup()
	err := s.parsePeers()
	assert.Nil(err)
	s.currentTerm.Store(1)

	responseChan := make(chan RPCResponse, 1)
	request := RPCRequest{
		RPCType:      PreVoteRequest,
		Request:      &raftypb.PreVoteRequest{Id: s.id, CurrentTerm: s.currentTerm.Load()},
		ResponseChan: responseChan,
	}
	s.wg.Add(3)
	go func() {
		defer s.wg.Done()
		for {
			select {
			case s.rpcPreVoteRequestChan <- request:
			case <-time.After(500 * time.Millisecond):
				return
			}
		}
	}()

	go func() {
		defer s.wg.Done()
		time.Sleep(100 * time.Millisecond) // Ensure the request is sent before draining
		s.drainPreVoteRequests()
	}()

	go func() {
		defer s.wg.Done()
		for {
			select {
			case data := <-responseChan:
				response := data.Response.(*raftypb.PreVoteResponse)
				if response != nil {
					assert.Equal(false, response.Granted)
				}
			case <-time.After(500 * time.Millisecond):
				return
			}
		}
	}()
	s.wg.Wait()
}

func TestDrainVoteRequests(t *testing.T) {
	assert := assert.New(t)

	s := basicNodeSetup()
	err := s.parsePeers()
	assert.Nil(err)
	s.currentTerm.Store(1)

	responseChan := make(chan RPCResponse, 1)
	request := RPCRequest{
		RPCType: VoteRequest,
		Request: &raftypb.VoteRequest{
			CandidateId:      s.id,
			CandidateAddress: s.Address.String(),
			CurrentTerm:      s.currentTerm.Load(),
		},
		ResponseChan: responseChan,
	}
	s.wg.Add(3)
	go func() {
		defer s.wg.Done()
		for {
			select {
			case s.rpcVoteRequestChan <- request:
			case <-time.After(500 * time.Millisecond):
				return
			}
		}
	}()

	go func() {
		defer s.wg.Done()
		time.Sleep(100 * time.Millisecond) // Ensure the request is sent before draining
		s.drainVoteRequests()
	}()

	go s.drainVoteRequests()
	go func() {
		defer s.wg.Done()
		for {
			select {
			case data := <-responseChan:
				response := data.Response.(*raftypb.VoteResponse)
				if response != nil {
					assert.Equal(false, response.Granted)
				}
			case <-time.After(500 * time.Millisecond):
				return
			}
		}
	}()
	s.wg.Wait()
}

func TestDrainAppendEntriesRequests(t *testing.T) {
	assert := assert.New(t)

	s := basicNodeSetup()
	err := s.parsePeers()
	assert.Nil(err)
	s.currentTerm.Store(1)

	responseChan := make(chan RPCResponse, 1)
	request := RPCRequest{
		RPCType:      AppendEntryRequest,
		Request:      &raftypb.AppendEntryRequest{},
		ResponseChan: responseChan,
	}

	s.wg.Add(3)
	go func() {
		defer s.wg.Done()
		for {
			select {
			case s.rpcAppendEntriesRequestChan <- request:
			case <-time.After(500 * time.Millisecond):
				return
			}
		}
	}()

	go func() {
		defer s.wg.Done()
		time.Sleep(100 * time.Millisecond) // Ensure the request is sent before draining
		s.drainAppendEntriesRequests()
	}()

	go s.drainAppendEntriesRequests()
	go func() {
		defer s.wg.Done()
		for {
			select {
			case data := <-responseChan:
				response := data.Response.(*raftypb.AppendEntryResponse)
				if response != nil {
					assert.Equal(false, response.Success)
				}
			case <-time.After(500 * time.Millisecond):
				return
			}
		}
	}()
	s.wg.Wait()
}

func TestDrainMembershipChangeRequests(t *testing.T) {
	assert := assert.New(t)

	s := basicNodeSetup()
	err := s.parsePeers()
	assert.Nil(err)
	s.currentTerm.Store(1)

	responseChan := make(chan RPCResponse, 1)
	request := RPCRequest{
		RPCType: MembershipChangeRequest,
		Request: RPCMembershipChangeRequest{
			Id:           s.id,
			Address:      s.Address.String(),
			Action:       uint32(Add),
			LastLogIndex: s.lastLogIndex.Load(),
			LastLogTerm:  s.lastLogTerm.Load(),
		},
		Timeout:      time.Second,
		ResponseChan: responseChan,
	}
	s.wg.Add(3)
	go func() {
		defer s.wg.Done()
		for {
			select {
			case s.rpcMembershipChangeRequestChan <- request:
			case <-time.After(500 * time.Millisecond):
				return
			}
		}
	}()

	go func() {
		defer s.wg.Done()
		time.Sleep(100 * time.Millisecond) // Ensure the request is sent before draining
		s.drainMembershipChangeRequests()
	}()

	go s.drainMembershipChangeRequests()
	go func() {
		defer s.wg.Done()
		for {
			select {
			case <-responseChan:
			case <-time.After(500 * time.Millisecond):
				return
			}
		}
	}()
	s.wg.Wait()
}
