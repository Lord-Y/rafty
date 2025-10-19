package cluster

import (
	"encoding/json"
	"net/http"

	"github.com/Lord-Y/rafty"
	"github.com/gin-gonic/gin"
)

// createKV will create a k/v with the provided data
// by sending a raft log.
// The ApplyCommand will then add the k/v
// to the k/v store
func (cc *Cluster) createKV(c *gin.Context) {
	var data KV

	if err := c.ShouldBind(&data); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if err := cc.submitCommandKVWrite(kvCommandSet, &data); err != nil {
		// we put this dummy error but we should return a
		// more appropriate one based on the situation
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "OK"})
}

// fetchKV will fetch k/v from the k/v store
func (cc *Cluster) fetchKV(c *gin.Context) {
	data := KV{Key: c.Params.ByName("name")}

	if cc.rafty.IsLeader() && c.Query("lease") == "true" {
		result, err := cc.fsm.memoryStore.kvGet([]byte(data.Key))
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		if result == nil {
			c.JSON(http.StatusNotFound, gin.H{"error": rafty.ErrKeyNotFound.Error()})
			return
		}

		data.Value = string(result)
		c.JSON(http.StatusOK, data)
		return
	}

	result, err := cc.submitCommandKVRead(kvCommandGet, &data)
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

// deleteKV will delete the provided k/v by sending a write command.
// The ApplyCommand will then delete the k/v
// from the k/v store
func (cc *Cluster) deleteKV(c *gin.Context) {
	data := KV{Key: c.Params.ByName("name")}

	if cc.fsm.memoryStore.kvExist([]byte(data.Key)) {
		if err := cc.submitCommandKVWrite(kvCommandDelete, &data); err != nil {
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

// fetchKVs will fetch all k/v from k/v store
func (cc *Cluster) fetchKVs(c *gin.Context) {
	var data []*KV

	if cc.rafty.IsLeader() && c.Query("lease") == "true" {
		result, err := cc.fsm.memoryStore.kvGetAll()
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		if result == nil {
			data = []*KV{}
		} else {
			data = result
		}

		c.JSON(http.StatusOK, data)
		return
	}

	result, err := cc.submitCommandKVRead(kvCommandGetAll, &KV{})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	if result == nil {
		data = []*KV{}
		c.JSON(http.StatusOK, data)
		return
	}

	if err := json.Unmarshal(result, &data); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, data)
}
