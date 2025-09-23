package rafty

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLogInMemory(t *testing.T) {
	assert := assert.New(t)

	t.Run("in_memory_store_logs", func(t *testing.T) {
		store := NewInMemoryStorage()
		defer func() {
			assert.Nil(store.Close())
		}()

		var logs []*LogEntry
		for index := range 100 {
			logs = append(logs, &LogEntry{
				Index: uint64(index),
				Term:  1,
			})
		}

		assert.Nil(store.StoreLogs(logs))
		assert.Nil(store.StoreLog(&LogEntry{Index: 100, Term: 1}))
	})

	t.Run("in_memory_get_log_by_index", func(t *testing.T) {
		store := NewInMemoryStorage()
		defer func() {
			assert.Nil(store.Close())
		}()

		index := uint64(100)
		_, err := store.GetLogByIndex(index)
		assert.Error(err)
		assert.Nil(store.StoreLog(&LogEntry{Index: index, Term: 1}))
		result, err := store.GetLogByIndex(index)
		assert.Nil(err)
		assert.Equal(index, result.Index)
	})

	t.Run("in_memory_get_logs_by_range", func(t *testing.T) {
		store := NewInMemoryStorage()
		defer func() {
			assert.Nil(store.Close())
		}()

		var logs []*LogEntry
		for index := range 100 {
			logs = append(logs, &LogEntry{
				Index: uint64(index),
				Term:  1,
			})
		}

		min, max, maxAppendEntries := uint64(0), uint64(50), uint64(10)
		response := store.GetLogsByRange(min, max, maxAppendEntries)
		assert.Error(response.Err)

		assert.Nil(store.StoreLogs(logs))
		response = store.GetLogsByRange(min, max, maxAppendEntries)
		assert.Nil(response.Err)
		assert.Greater(response.Total, maxAppendEntries)
		assert.Equal(true, response.SendSnapshot)
	})

	t.Run("in_memory_get_last_configuration", func(t *testing.T) {
		store := NewInMemoryStorage()
		defer func() {
			assert.Nil(store.Close())
		}()

		enc := EncodePeers([]Peer{{Address: "127.0.0.1:60000", ID: "60"}, {Address: "127.0.0.1:61000", ID: "61"}, {Address: "127.0.0.1:62000", ID: "62"}})
		log := &LogEntry{
			LogType: uint32(LogConfiguration),
			Index:   0,
			Term:    1,
			Command: enc,
		}

		_, err := store.GetLastConfiguration()
		assert.Error(err)

		assert.Nil(store.StoreLog(log))
		_, err = store.GetLastConfiguration()
		assert.Nil(err)
	})

	t.Run("in_memory_discard_logs", func(t *testing.T) {
		store := NewInMemoryStorage()
		defer func() {
			assert.Nil(store.Close())
		}()

		max := uint64(100)
		assert.Error(store.DiscardLogs(0, max))

		var logs []*LogEntry
		for index := range max {
			logs = append(logs, &LogEntry{
				Index: index,
				Term:  1,
			})
		}

		assert.Nil(store.StoreLogs(logs))
		assert.Nil(store.DiscardLogs(0, max-1))
	})

	t.Run("in_memory_first_index_last_index", func(t *testing.T) {
		store := NewInMemoryStorage()
		defer func() {
			assert.Nil(store.Close())
		}()

		var logs []*LogEntry
		for index := range uint64(10) {
			logs = append(logs, &LogEntry{
				Index: index,
				Term:  1,
			})
		}

		first, last := uint64(0), uint64(9)
		_, err := store.FirstIndex()
		assert.Error(err)
		_, err = store.LastIndex()
		assert.Error(err)

		assert.Nil(store.StoreLogs(logs))

		f, err := store.FirstIndex()
		assert.Nil(err)
		assert.Equal(first, f)
		l, err := store.LastIndex()
		assert.Nil(err)
		assert.Equal(last, l)
	})
}
