package rafty

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStateString(t *testing.T) {
	assert := assert.New(t)

	tests := []State{
		Down,
		Follower,
		Candidate,
		Leader,
	}
	results := []string{
		"down",
		"follower",
		"candidate",
		"leader",
	}

	for k, v := range tests {
		assert.Equal(v.String() == results[k], true)
	}
}

func TestStateUpOrDownString(t *testing.T) {
	assert := assert.New(t)

	tests := []upOrDown{
		stepDown,
		stepUp,
	}
	results := []string{
		"down",
		"up",
	}

	for k, v := range tests {
		assert.Equal(v.String() == results[k], true)
	}
}
