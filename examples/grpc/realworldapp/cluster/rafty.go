package cluster

import (
	"time"

	"github.com/Lord-Y/rafty"
)

func (c *Cluster) newRafty() (*rafty.Rafty, error) {
	options := rafty.Options{
		Logger:             c.Logger,
		DataDir:            c.dataDir,
		MinimumClusterSize: 3,
		IsVoter:            true,
		InitialPeers:       c.buildPeers(),
		SnapshotInterval:   30 * time.Second,
	}

	store, err := c.buildStore()
	if err != nil {
		return nil, err
	}

	c.fsm = newFSMState(store)
	snapshotConfig := newSnapshot(c.dataDir, 3)

	return rafty.NewRafty(c.address, c.id, options, store, store, c.fsm, snapshotConfig)
}

func (c *Cluster) startRafty() {
	go func() {
		if err := c.rafty.Start(); err != nil {
			c.Logger.Fatal().Err(err).Msg("Fail to start node")
		}
	}()
}

func (c *Cluster) stopRafty() {
	c.rafty.Stop()
}
