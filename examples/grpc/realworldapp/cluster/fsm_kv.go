package cluster

import (
	"github.com/Lord-Y/rafty"
)

// kvSet will add key/value to the k/v store.
// An error will be returned if necessary
func (m *memoryStore) kvSet(key, value []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.kv[string(key)] = value
	return nil
}

// kvGet will fetch provided key from the k/v store.
// An error will be returned if the key is not found
func (m *memoryStore) kvGet(key []byte) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if val, ok := m.kv[string(key)]; ok {
		return val, nil
	}
	return nil, rafty.ErrKeyNotFound
}

// kvExist will return true if the k/v exist
func (m *memoryStore) kvExist(key []byte) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if _, ok := m.kv[string(key)]; ok {
		return true
	}
	return false
}

// kvDelete will delete provided key from the k/v store.
// An error will be returned if the key is not found
func (m *memoryStore) kvDelete(key []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.kv, string(key))
}

// kvGetAll will fetch all kv from the k/v store.
// An error will be returned if the any
func (m *memoryStore) kvGetAll() (u [][]byte, err error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.kv) == 0 {
		return nil, nil
	}

	for _, v := range m.kv {
		u = append(u, v)
	}
	return
}
