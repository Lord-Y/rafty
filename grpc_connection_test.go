package rafty

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGrpcConnection_getClient(t *testing.T) {
	assert := assert.New(t)

	t.Run("not_nil", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		client := s.connectionManager.getClient(s.Address.String())
		assert.NotNil(client)
		// fetch the client again to ensure it is cached
		client = s.connectionManager.getClient(s.Address.String())
		assert.NotNil(client)
	})

	t.Run("disconnect_nil", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		client := s.connectionManager.getClient(s.Address.String())
		assert.NotNil(client)
		s.connectionManager.disconnectAllPeers()
		client = s.connectionManager.getClient(s.Address.String())
		assert.Nil(client)
	})

	t.Run("leadership_transfer", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		client := s.connectionManager.getClient(s.Address.String())
		assert.NotNil(client)
		s.connectionManager.disconnectAllPeers()
		s.leadershipTransferInProgress.Store(true)
		client = s.connectionManager.getClient(s.Address.String())
		assert.NotNil(client)
	})
}
