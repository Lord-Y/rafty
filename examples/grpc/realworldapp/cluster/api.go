package cluster

import (
	"context"
	"fmt"
	"net/http"
	"time"
)

// newAPIServer will build the api server config
func (c *Cluster) newAPIServer() {
	c.apiServer = &http.Server{
		Addr:    fmt.Sprintf(":%d", c.HTTPPort),
		Handler: c.newApiRouters(),
	}
}

// startAPI will start the api server
func (c *Cluster) startAPIServer() {
	go func() {
		if err := c.apiServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			c.Logger.Fatal().Err(err).Msg("Startup api server failed")
		}
	}()
	// <-c.quit
}

// stopAPIServer will stop the api server
func (c *Cluster) stopAPIServer() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if err := c.apiServer.Shutdown(ctx); err != nil {
		c.Logger.Fatal().Err(err).Msg("API server shutted down abruptly")
	}
}
