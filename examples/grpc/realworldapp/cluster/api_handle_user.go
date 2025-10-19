package cluster

import (
	"encoding/json"
	"net/http"

	"github.com/Lord-Y/rafty"
	"github.com/gin-gonic/gin"
)

// createUser will create a user with the provided data
// by sending a raft log.
// The ApplyCommand will then add the user
// to the users store
func (cc *Cluster) createUser(c *gin.Context) {
	var data User

	if err := c.ShouldBind(&data); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if err := cc.submitCommandUserWrite(userCommandSet, &data); err != nil {
		// we put this dummy error but we should return a
		// more appropriate one based on the situation
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "OK"})
}

// fetchUser will fetch user from the users store
func (cc *Cluster) fetchUser(c *gin.Context) {
	data := User{Firstname: c.Params.ByName("name")}

	if cc.rafty.IsLeader() && c.Query("lease") == "true" {
		result, err := cc.fsm.memoryStore.usersGet([]byte(data.Firstname))
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		if result == nil {
			c.JSON(http.StatusNotFound, gin.H{"error": rafty.ErrKeyNotFound.Error()})
			return
		}

		data.Lastname = string(result)
		c.JSON(http.StatusOK, data)
		return
	}

	result, err := cc.submitCommandUserRead(userCommandGet, &data)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	if result == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": rafty.ErrKeyNotFound.Error()})
		return
	}

	if err := json.Unmarshal(result, &data); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, data)
}

// deleteUser will delete the provided user by sending a write command.
// The ApplyCommand will then delete the user
// from the users store
func (cc *Cluster) deleteUser(c *gin.Context) {
	data := User{Firstname: c.Params.ByName("name")}

	if cc.fsm.memoryStore.usersExist([]byte(data.Firstname)) {
		if err := cc.submitCommandUserWrite(userCommandDelete, &data); err != nil {
			// we put this dummy error but we should return a
			// more appropriate one based on the situation
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"message": "OK"})
		return
	}

	c.JSON(http.StatusNotFound, gin.H{"error": rafty.ErrKeyNotFound.Error()})
}

// fetchUsers will fetch all users from users store
func (cc *Cluster) fetchUsers(c *gin.Context) {
	var data []*User

	if cc.rafty.IsLeader() && c.Query("lease") == "true" {
		result, err := cc.fsm.memoryStore.usersGetAll()
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		if result == nil {
			data = []*User{}
		} else {
			data = result
		}

		c.JSON(http.StatusOK, data)
		return
	}

	result, err := cc.submitCommandUserRead(userCommandGetAll, &User{})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	if result == nil {
		data = []*User{}
		c.JSON(http.StatusOK, data)
		return
	}

	if err := json.Unmarshal(result, &data); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, data)
}
