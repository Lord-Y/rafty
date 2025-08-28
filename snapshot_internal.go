package rafty

import (
	"errors"
	"fmt"
)

// snapshotTestHook is used for injecting errors during unit testing
var snapshotTestHook func() error

// takeSnapshot will take a new snapshot
// if all requirements are reached
func (r *Rafty) takeSnapshot() (string, error) {
	r.wg.Add(1)
	defer r.wg.Done()

	if snapshotTestHook != nil {
		return "", snapshotTestHook()
	}

	lastLogIndex := r.lastLogIndex.Load()
	lastLogTerm := r.lastLogTerm.Load()
	if r.options.SnapshotThreshold > (lastLogIndex - r.lastIncludedIndex.Load()) {
		return "", ErrNoSnapshotToTake
	}

	lastAppliedConfig, err := r.logStore.GetLogByIndex(r.lastAppliedConfigIndex.Load())
	if err != nil != errors.Is(err, ErrLogNotFound) {
		return "", err
	}

	var config Configuration
	if !errors.Is(err, ErrLogNotFound) {
		config.ServerMembers, _ = decodePeers(lastAppliedConfig.Command)
	} else {
		config.ServerMembers, _ = r.getAllPeers()
	}

	snapshot, err := r.snapshot.PrepareSnapshotWriter(lastLogIndex, lastLogTerm, r.lastAppliedConfigIndex.Load(), r.lastAppliedConfigTerm.Load(), config)
	if err != nil {
		defer func() {
			if err := snapshot.Discard(); err != nil {
				r.Logger.Error().Err(err).
					Str("address", r.Address.String()).
					Str("id", r.id).
					Str("state", r.getState().String()).
					Str("snapshotName", snapshot.Name()).
					Msgf("Fail to discard snapshot")
			}
		}()
		return "", err
	}

	if err := r.fsm.Snapshot(snapshot); err != nil {
		defer func() {
			if err := snapshot.Discard(); err != nil {
				r.Logger.Error().Err(err).
					Str("address", r.Address.String()).
					Str("id", r.id).
					Str("state", r.getState().String()).
					Str("snapshotName", snapshot.Name()).
					Msgf("Fail to discard snapshot")
			}
		}()
		return snapshot.Name(), err
	}

	if err := snapshot.Close(); err != nil {
		defer func() {
			if err := snapshot.Discard(); err != nil {
				r.Logger.Error().Err(err).
					Str("address", r.Address.String()).
					Str("id", r.id).
					Str("state", r.getState().String()).
					Str("snapshotName", snapshot.Name()).
					Msgf("Fail to discard snapshot")
			}
		}()
		return snapshot.Name(), err
	}

	r.lastIncludedIndex.Store(lastLogIndex)
	r.lastIncludedTerm.Store(lastLogTerm)
	r.Logger.Info().
		Str("address", r.Address.String()).
		Str("id", r.id).
		Str("state", r.getState().String()).
		Str("snapshotName", snapshot.Name()).
		Str("lastIncludedIndex", fmt.Sprintf("%d", lastLogIndex)).
		Str("lastIncludedTerm", fmt.Sprintf("%d", lastLogTerm)).
		Msgf("Snapshot successfully taken")
	return snapshot.Name(), nil
}
