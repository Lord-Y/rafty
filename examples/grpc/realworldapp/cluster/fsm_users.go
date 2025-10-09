package cluster

import (
	"bytes"

	"github.com/Lord-Y/rafty"
)

// usersSet will add key/value to the users store.
// An error will be returned if necessary
func (m *memoryStore) usersSet(log *rafty.LogEntry, key, value []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// delete existing key if exist
	// That will allows us to cleanly perform snapshots
	// when required by removed overriden keys and reduce
	// disk space and amount of time to restore data
	keyName := string(key)
	if _, ok := m.users[keyName]; ok {
		delete(m.logs, m.users[keyName].index)
	}

	m.logs[log.Index] = log
	m.users[keyName] = data{index: log.Index, value: value}
	return nil
}

// usersGet will fetch provided key from the users store.
// An error will be returned if the key is not found
func (m *memoryStore) usersGet(key []byte) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if val, ok := m.users[string(key)]; ok {
		return val.value, nil
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

	data := string(key)
	if _, ok := m.users[data]; ok {
		delete(m.logs, m.users[data].index)
		delete(m.kv, string(key))
	}
}

// usersGetAll will fetch all users from the users store.
// An error will be returned if the any
func (m *memoryStore) usersGetAll() (z []*User, err error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.users) == 0 {
		return nil, nil
	}

	for k, v := range m.users {
		z = append(z, &User{Firstname: k, Lastname: string(v.value)})
	}
	return
}

// usersEncoded will fetch all users from the users store.
// users will be binary encoded when command is forwarded
// to the leader.
// An error will be returned if the any
func (m *memoryStore) usersEncoded() (u []byte, err error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.users) == 0 {
		return nil, nil
	}

	bufferChecksum := new(bytes.Buffer)
	for k, v := range m.users {
		buffer := new(bytes.Buffer)
		data := User{
			Firstname: string(k),
			Lastname:  string(v.value),
		}

		if err := userEncodeCommand(userCommand{Kind: userCommandGetAll, Key: data.Firstname, Value: data.Lastname}, buffer); err != nil {
			return nil, err
		}

		if err = rafty.MarshalBinaryWithChecksum(buffer, bufferChecksum); err != nil {
			return nil, err
		}
	}
	return bufferChecksum.Bytes(), nil
}
