package cluster

import "github.com/Lord-Y/rafty"

// logsApplyCommand will only store LogNoop or LogConfiguration
func (m *memoryStore) logsApplyCommand(log *rafty.LogEntry) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.logs[log.Index] = log
	return nil, nil
}
