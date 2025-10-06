package cluster

import (
	"bytes"
	"time"

	"github.com/Lord-Y/rafty"
)

// submitCommandUserWrite will send a write command to the leader
func (c *Cluster) submitCommandUserWrite(kind commandKind, data *User) error {
	buffer := new(bytes.Buffer)
	if err := userEncodeCommand(userCommand{Kind: kind, Key: data.Firstname, Value: data.Lastname}, buffer); err != nil {
		return err
	}

	if _, err := c.rafty.SubmitCommand(time.Second, rafty.LogReplication, buffer.Bytes()); err != nil {
		return err
	}
	return nil
}

// submitCommandKVWrite will send a write command to the leader
func (c *Cluster) submitCommandKVWrite(kind commandKind, data *KV) error {
	buffer := new(bytes.Buffer)
	if err := kvEncodeCommand(kvCommand{Kind: kind, Key: data.Key, Value: data.Value}, buffer); err != nil {
		return err
	}

	if _, err := c.rafty.SubmitCommand(time.Second, rafty.LogReplication, buffer.Bytes()); err != nil {
		return err
	}
	return nil
}
