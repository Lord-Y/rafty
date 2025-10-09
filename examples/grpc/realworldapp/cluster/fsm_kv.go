package cluster

import (
	"bytes"

	"github.com/Lord-Y/rafty"
)

// kvSet will add key/value to the k/v store.
// An error will be returned if necessary
func (m *memoryStore) kvSet(log *rafty.LogEntry, key, value []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// delete existing key if exist
	// That will allows us to cleanly perform snapshots
	// when required by removed overriden keys and reduce
	// disk space and amount of time to restore data
	keyName := string(key)
	if _, ok := m.kv[keyName]; ok {
		delete(m.logs, m.kv[keyName].index)
	}

	m.logs[log.Index] = log
	m.kv[keyName] = data{index: log.Index, value: value}
	return nil
}

// kvGet will fetch provided key from the k/v store.
// An error will be returned if the key is not found
func (m *memoryStore) kvGet(key []byte) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if val, ok := m.kv[string(key)]; ok {
		return val.value, nil
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

	data := string(key)
	if _, ok := m.kv[data]; ok {
		delete(m.logs, m.kv[data].index)
		delete(m.kv, string(key))
	}
}

// kvGetAll will fetch all kv from the k/v store.
// An error will be returned if the any
func (m *memoryStore) kvGetAll() (z []*KV, err error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.kv) == 0 {
		return nil, nil
	}

	for k, v := range m.kv {
		z = append(z, &KV{Key: k, Value: string(v.value)})
	}
	return
}

// kvsEncoded will fetch all users from the kv store.
// kv will be binary encoded when command is forwarded
// to the leader.
// An error will be returned if the any
func (m *memoryStore) kvsEncoded() (u []byte, err error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.kv) == 0 {
		return nil, nil
	}

	bufferChecksum := new(bytes.Buffer)
	for k, v := range m.kv {
		buffer := new(bytes.Buffer)
		data := KV{
			Key:   string(k),
			Value: string(v.value),
		}

		if err := kvEncodeCommand(kvCommand{Kind: kvCommandGetAll, Key: data.Key, Value: data.Value}, buffer); err != nil {
			return nil, err
		}

		if err = rafty.MarshalBinaryWithChecksum(buffer, bufferChecksum); err != nil {
			return nil, err
		}
	}
	return bufferChecksum.Bytes(), nil
}
