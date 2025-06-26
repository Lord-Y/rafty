package rafty

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGrpcConnection_getClient(t *testing.T) {
	assert := assert.New(t)

	t.Run("client_not_nil", func(t *testing.T) {
		s := basicNodeSetup()
		err := s.parsePeers()
		assert.Nil(err)
		client := s.connectionManager.getClient(s.Address.String(), s.id)
		assert.NotNil(client)
		// fetch the client again to ensure it is cached
		client = s.connectionManager.getClient(s.Address.String(), s.id)
		assert.NotNil(client)
	})

	t.Run("client_disconnect_nil", func(t *testing.T) {
		s := basicNodeSetup()
		err := s.parsePeers()
		assert.Nil(err)
		client := s.connectionManager.getClient(s.Address.String(), s.id)
		assert.NotNil(client)
		s.connectionManager.disconnectAllPeers()
		client = s.connectionManager.getClient(s.Address.String(), s.id)
		assert.Nil(client)
	})

	t.Run("client_leadership_transfer", func(t *testing.T) {
		s := basicNodeSetup()
		err := s.parsePeers()
		assert.Nil(err)
		client := s.connectionManager.getClient(s.Address.String(), s.id)
		assert.NotNil(client)
		s.connectionManager.disconnectAllPeers()
		s.leadershipTransferInProgress.Store(true)
		client = s.connectionManager.getClient(s.Address.String(), s.id)
		assert.NotNil(client)
	})
}
