package cluster

// Start will start the cluster
func (c *Cluster) Start() error {
	c.buildAddressAndID()
	c.buildDataDir()
	c.buildSignal()

	var err error
	c.rafty, err = c.newRafty()
	if err != nil {
		return err
	}
	c.newAPIServer()

	c.startRafty()
	c.startAPIServer()
	c.Logger.Info().Msg("server started successfully")

	<-c.quit

	c.stopAPIServer()
	c.stopRafty()
	c.Logger.Info().Msg("server stopped successfully")

	return nil
}
