package cluster

import (
	"bytes"

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
func (m *memoryStore) usersGetAll() (z []*User, err error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.users) == 0 {
		return nil, nil
	}

	for k, v := range m.users {
		z = append(z, &User{Firstname: k, Lastname: string(v)})
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
			Lastname:  string(v),
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
