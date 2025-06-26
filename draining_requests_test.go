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

	responseChan := make(chan *raftypb.PreVoteResponse, 1)
	request := preVoteResquestWrapper{
		request:      &raftypb.PreVoteRequest{Id: s.id, CurrentTerm: s.currentTerm.Load()},
		responseChan: responseChan,
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
				assert.Equal(false, data.Granted)
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

	responseChan := make(chan *raftypb.VoteResponse, 1)
	request := voteResquestWrapper{
		request: &raftypb.VoteRequest{
			CandidateId:      s.id,
			CandidateAddress: s.Address.String(),
			CurrentTerm:      s.currentTerm.Load(),
		},
		responseChan: responseChan,
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
				assert.Equal(false, data.Granted)
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

	responseChan := make(chan *raftypb.AppendEntryResponse, 1)
	request := appendEntriesResquestWrapper{
		request:      &raftypb.AppendEntryRequest{},
		responseChan: responseChan,
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
				assert.Equal(false, data.Success)
			case <-time.After(500 * time.Millisecond):
				return
			}
		}
	}()
	s.wg.Wait()
}
