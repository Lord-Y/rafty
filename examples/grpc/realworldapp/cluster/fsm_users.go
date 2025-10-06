package cluster

import (
	"github.com/Lord-Y/rafty"
)

// usersSet will add key/value to the users store.
// An error will be returned if necessary
func (m *memoryStore) usersSet(key, value []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.users[string(key)] = value
	return nil
}

// usersGet will fetch provided key from the users store.
// An error will be returned if the key is not found
func (m *memoryStore) usersGet(key []byte) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if val, ok := m.users[string(key)]; ok {
		return val, nil
	}
	return nil, rafty.ErrKeyNotFound
}

// usersExist will return true if the user exist
func (m *memoryStore) usersExist(key []byte) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if _, ok := m.users[string(key)]; ok {
		return true
	}
	return false
}

// usersDelete will delete provided key from the users store.
// An error will be returned if the key is not found
func (m *memoryStore) usersDelete(key []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.users, string(key))
}

// usersGetAll will fetch all users from the users store.
// An error will be returned if the any
func (m *memoryStore) usersGetAll() (u [][]byte, err error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.users) == 0 {
		return nil, nil
	}

	for _, v := range m.users {
		u = append(u, v)
	}
	return
}
