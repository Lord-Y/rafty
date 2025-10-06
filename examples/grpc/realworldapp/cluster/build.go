package cluster

import (
	"fmt"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/Lord-Y/rafty"
	bolt "go.etcd.io/bbolt"
)

// buildAddressAndID will build the node address and id
func (c *Cluster) buildAddressAndID() {
	c.address = net.TCPAddr{
		IP:   net.ParseIP(c.Host),
		Port: c.GRPCPort,
	}

	c.id = fmt.Sprintf("%d", c.address.Port)
}

// buildPeers will build the initial peer members of the cluster
func (c *Cluster) buildPeers() []rafty.InitialPeer {
	peers := []rafty.InitialPeer{{Address: c.address.String()}}

	for _, v := range c.Members {
		peers = append(peers, rafty.InitialPeer{Address: v})
	}

	return peers
}

// buildDataDir will build the working dir of the current node
func (c *Cluster) buildDataDir() {
	c.dataDir = filepath.Join(os.TempDir(), "rafty", "realworldapp", c.id)
}

// buildStore will build the bolt store
func (c *Cluster) buildStore() (*rafty.BoltStore, error) {
	storeOptions := rafty.BoltOptions{
		DataDir: c.dataDir,
		Options: bolt.DefaultOptions,
	}

	return rafty.NewBoltStorage(storeOptions)
}

// buildSignal will build the signal required to stop the cluster
func (c *Cluster) buildSignal() {
	c.quit = make(chan os.Signal, 1)
	// kill (no param) default send syscanll.SIGTERM
	// kill -2 is syscall.SIGINT
	// kill -9 is syscall. SIGKILL but can"t be catch, so don't need add it
	signal.Notify(c.quit, syscall.SIGINT, syscall.SIGTERM)
}
