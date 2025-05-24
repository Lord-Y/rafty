package rafty

type down struct {
	// rafty holds rafty config
	rafty *Rafty
}

// init initialize all requirements needed by
// the current node type
func (r *down) init() {}

// onTimeout permit to reset election timer
// and then perform some other actions
func (r *down) onTimeout() {}

// release permit to cancel or gracefully some actions
// when the node change state
func (r *down) release() {}
