package cluster

import (
	"github.com/gin-contrib/requestid"
	"github.com/gin-gonic/gin"
)

// newApiRouters will return the api router
func (c *Cluster) newApiRouters() *gin.Engine {
	gin.DisableConsoleColor()
	gin.SetMode(gin.ReleaseMode)

	router := gin.New()
	router.Use(requestid.New())
	router.Use(gin.Recovery())

	v1 := router.Group("/api/v1")
	{
		v1.GET("/user/:name", c.fetchUser)
		v1.POST("/user", c.createUser)
		v1.DELETE("/user/:name", c.deleteUser)
		v1.GET("/users", c.fetchUsers)

		v1.GET("/kv/:name", c.fetchKV)
		v1.POST("/kv", c.createKV)
		v1.DELETE("/kv/:name", c.deleteKV)
		v1.GET("/kvs", c.fetchKVs)
	}
	return router
}
