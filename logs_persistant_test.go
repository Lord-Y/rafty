package rafty

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.etcd.io/bbolt"
)

func TestLogsPersistant(t *testing.T) {
	assert := assert.New(t)

	t.Run("new_bolt_storage_no_datadir", func(t *testing.T) {
		boltOptions := BoltOptions{
			Options: bbolt.DefaultOptions,
		}
		_, err := NewBoltStorage(boltOptions)
		assert.ErrorIs(err, ErrDataDirRequired)
	})

	t.Run("new_bolt_storage_false_datadir", func(t *testing.T) {
		boltOptions := BoltOptions{
			DataDir: filepath.Join(os.TempDir(), "rafty_test", "bolt", "new_bolt_storage_false_datadir"),
			Options: bbolt.DefaultOptions,
		}
		defer func() {
			assert.Nil(os.RemoveAll(boltOptions.DataDir))
		}()

		_ = createDirectoryIfNotExist(boltOptions.DataDir, 0750)
		_, _ = os.Create(filepath.Join(boltOptions.DataDir, "db"))
		_, err := NewBoltStorage(boltOptions)
		assert.Error(err)
	})

	t.Run("new_bolt_storage", func(t *testing.T) {
		boltOptions := BoltOptions{
			DataDir: filepath.Join(os.TempDir(), "rafty_test", "bolt", "new_bolt_storage"),
			Options: bbolt.DefaultOptions,
		}
		defer func() {
			assert.Nil(os.RemoveAll(boltOptions.DataDir))
		}()

		store, err := NewBoltStorage(boltOptions)
		assert.Nil(err)
		assert.Nil(store.Close())
	})

	t.Run("new_bolt_storage_initialize_buckets", func(t *testing.T) {
		boltOptions := BoltOptions{
			DataDir: filepath.Join(os.TempDir(), "rafty_test", "bolt", "initialize_buckets"),
			Options: bbolt.DefaultOptions,
		}
		defer func() {
			assert.Nil(os.RemoveAll(boltOptions.DataDir))
		}()

		store, err := NewBoltStorage(boltOptions)
		assert.Nil(err)
		assert.Nil(store.Close())
		assert.Error(store.initializeBuckets())

		store, err = NewBoltStorage(boltOptions)
		assert.Nil(err)
		assert.Nil(store.Close())
	})

	t.Run("new_bolt_storage_store_get_logs", func(t *testing.T) {
		boltOptions := BoltOptions{
			DataDir: filepath.Join(os.TempDir(), "rafty_test", "bolt", "sotre_get_logs"),
			Options: bbolt.DefaultOptions,
		}
		defer func() {
			assert.Nil(os.RemoveAll(boltOptions.DataDir))
		}()

		store, err := NewBoltStorage(boltOptions)
		assert.Nil(err)

		index := uint64(1)
		entry := &LogEntry{
			Index: index,
			Term:  1,
		}

		assert.Nil(store.Close())
		assert.Error(store.StoreLog(entry))

		store, err = NewBoltStorage(boltOptions)
		assert.Nil(store.StoreLog(entry))
		assert.Nil(err)
		assert.Nil(store.Close())

		_, err = store.GetLogByIndex(index)
		assert.Error(err)

		store, err = NewBoltStorage(boltOptions)
		assert.Nil(err)
		got, err := store.GetLogByIndex(index)
		assert.Nil(err)
		assert.Equal(index, got.Index)

		_, err = store.GetLogByIndex(uint64(2))
		assert.Error(err)
		assert.Nil(store.Close())
	})

	t.Run("new_bolt_storage_get_logs_by_range", func(t *testing.T) {
		boltOptions := BoltOptions{
			DataDir: filepath.Join(os.TempDir(), "rafty_test", "bolt", "get_logs_by_range"),
			Options: bbolt.DefaultOptions,
		}
		defer func() {
			assert.Nil(os.RemoveAll(boltOptions.DataDir))
		}()

		store, err := NewBoltStorage(boltOptions)
		assert.Nil(err)

		var logs []*LogEntry
		for index := range 100 {
			logs = append(logs, &LogEntry{
				Index: uint64(index),
				Term:  1,
			})
		}

		min, max, maxAppendEntries := uint64(0), uint64(50), uint64(10)
		response := store.GetLogsByRange(min, max, maxAppendEntries)
		assert.Nil(response.Err)

		assert.Nil(store.StoreLogs(logs))
		response = store.GetLogsByRange(min, max, maxAppendEntries)
		assert.Greater(response.Total, maxAppendEntries)
		assert.Equal(true, response.SendSnapshot)
		assert.Nil(store.Close())
	})

	t.Run("new_bolt_storage_get_last_configuration", func(t *testing.T) {
		boltOptions := BoltOptions{
			DataDir: filepath.Join(os.TempDir(), "rafty_test", "bolt", "get_last_configuration"),
			Options: bbolt.DefaultOptions,
		}
		defer func() {
			assert.Nil(os.RemoveAll(boltOptions.DataDir))
		}()

		store, err := NewBoltStorage(boltOptions)
		assert.Nil(err)
		_, err = store.GetLastConfiguration()
		assert.Error(err)

		var logs []*LogEntry
		for index := range 50 {
			logs = append(logs, &LogEntry{
				Index: uint64(index),
				Term:  1,
			})
		}

		enc := encodePeers([]Peer{{Address: "127.0.0.1:60000", ID: "60"}, {Address: "127.0.0.1:61000", ID: "61"}, {Address: "127.0.0.1:62000", ID: "62"}})
		configIndex := uint64(50)
		logs = append(logs, &LogEntry{
			LogType: uint32(logConfiguration),
			Index:   configIndex,
			Term:    1,
			Command: enc,
		})

		for index := range 50 {
			logs = append(logs, &LogEntry{
				Index: uint64(index + 51),
				Term:  1,
			})
		}

		assert.Nil(store.StoreLogs(logs))
		response, err := store.GetLastConfiguration()
		assert.Nil(err)
		assert.Equal(configIndex, response.Index)
		assert.Nil(store.Close())
	})

	t.Run("new_bolt_storage_discard_logs", func(t *testing.T) {
		boltOptions := BoltOptions{
			DataDir: filepath.Join(os.TempDir(), "rafty_test", "bolt", "discard_logs"),
			Options: bbolt.DefaultOptions,
		}
		defer func() {
			assert.Nil(os.RemoveAll(boltOptions.DataDir))
		}()

		store, err := NewBoltStorage(boltOptions)
		assert.Nil(err)
		assert.Nil(store.Close())
		assert.Error(store.DiscardLogs(10, 20))

		var logs []*LogEntry
		for index := range 50 {
			logs = append(logs, &LogEntry{
				Index: uint64(index),
				Term:  1,
			})
		}

		store, err = NewBoltStorage(boltOptions)
		assert.Nil(err)
		assert.Nil(store.StoreLogs(logs))
		assert.Nil(store.DiscardLogs(10, 20))
		assert.Nil(store.DiscardLogs(100, 200))
		_, err = store.GetLogByIndex(20)
		assert.Error(err)
		assert.Nil(store.Close())
	})

	t.Run("new_bolt_storage_metadata", func(t *testing.T) {
		boltOptions := BoltOptions{
			DataDir: filepath.Join(os.TempDir(), "rafty_test", "bolt", "metadata"),
			Options: bbolt.DefaultOptions,
		}
		defer func() {
			assert.Nil(os.RemoveAll(boltOptions.DataDir))
		}()

		store, err := NewBoltStorage(boltOptions)
		assert.Nil(err)

		_, err = store.GetMetadata()
		assert.Error(err)

		s := basicNodeSetup()
		s.currentTerm.Store(1)
		s.lastApplied.Store(1)

		assert.Nil(store.storeMetadata(s.buildMetadata()))
		_, err = store.GetMetadata()
		assert.Nil(err)
		assert.Nil(store.Close())
	})

	t.Run("new_bolt_storage_kv_get_set", func(t *testing.T) {
		boltOptions := BoltOptions{
			DataDir: filepath.Join(os.TempDir(), "rafty_test", "bolt", "kv_get_set"),
			Options: bbolt.DefaultOptions,
		}
		defer func() {
			assert.Nil(os.RemoveAll(boltOptions.DataDir))
		}()

		store, err := NewBoltStorage(boltOptions)
		assert.Nil(err)

		key, value := "key", "value"

		_, err = store.Get([]byte(key))
		assert.Error(err)

		assert.Nil(store.Set([]byte(key), []byte(value)))
		foundKey, err := store.Get([]byte(key))
		assert.Nil(err)
		assert.Equal(value, string(foundKey))

		keyUint, valueUint := "keyUint", uint64(64)
		valUint := store.GetUint64([]byte(keyUint))
		assert.Equal(uint64(0), valUint)
		assert.Nil(store.SetUint64([]byte(keyUint), encodeUint64ToBytes(valueUint)))
		assert.Equal(valueUint, store.GetUint64([]byte(keyUint)))

		assert.Nil(store.Close())
	})

	t.Run("new_bolt_storage_first_last_index", func(t *testing.T) {
		boltOptions := BoltOptions{
			DataDir: filepath.Join(os.TempDir(), "rafty_test", "bolt", "first_last_index"),
			Options: bbolt.DefaultOptions,
		}
		defer func() {
			assert.Nil(os.RemoveAll(boltOptions.DataDir))
		}()

		store, err := NewBoltStorage(boltOptions)
		assert.Nil(err)
		assert.Nil(store.Close())

		_, err = store.FirstIndex()
		assert.Error(err)
		_, err = store.LastIndex()
		assert.Error(err)

		store, err = NewBoltStorage(boltOptions)
		assert.Nil(err)

		_, err = store.FirstIndex()
		assert.Error(err)
		_, err = store.LastIndex()
		assert.Error(err)

		var logs []*LogEntry
		for index := range 50 {
			logs = append(logs, &LogEntry{
				Index: uint64(index),
				Term:  1,
			})
		}
		assert.Nil(store.StoreLogs(logs))
		firstIndex, err := store.FirstIndex()
		assert.Nil(err)
		assert.Equal(uint64(0), firstIndex)

		lastIndex, err := store.LastIndex()
		assert.Nil(err)
		assert.Equal(uint64(49), lastIndex)

		assert.Nil(store.Close())
	})
}
