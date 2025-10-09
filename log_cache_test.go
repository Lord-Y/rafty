package rafty

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.etcd.io/bbolt"
)

func TestLogCache(t *testing.T) {
	assert := assert.New(t)

	t.Run("cache_store_logs", func(t *testing.T) {
		boltOptions := BoltOptions{
			DataDir: filepath.Join(os.TempDir(), "rafty_test", "cache_store_logs"),
			Options: bbolt.DefaultOptions,
		}

		defer func() {
			assert.Nil(os.RemoveAll(getRootDir(boltOptions.DataDir)))
		}()
		store, err := NewBoltStorage(boltOptions)
		assert.Nil(err)

		cacheOptions := LogCacheOptions{
			LogStore:     store,
			CacheOnWrite: true,
			TTL:          2 * time.Second,
		}
		cacheStore := NewLogCache(cacheOptions)
		defer func() {
			assert.Nil(os.RemoveAll(getRootDir(boltOptions.DataDir)))
			assert.Nil(cacheStore.Close())
		}()

		var logs []*LogEntry
		for index := range 100 {
			logs = append(logs, &LogEntry{
				Index: uint64(index),
				Term:  1,
			})
		}

		assert.Nil(cacheStore.StoreLogs(logs))
		assert.Nil(cacheStore.StoreLog(&LogEntry{Index: 100, Term: 1}))
	})

	t.Run("cache_get_log_by_index", func(t *testing.T) {
		boltOptions := BoltOptions{
			DataDir: filepath.Join(os.TempDir(), "rafty_test", "cache_get_log_by_index"),
			Options: bbolt.DefaultOptions,
		}

		defer func() {
			assert.Nil(os.RemoveAll(getRootDir(boltOptions.DataDir)))
		}()
		store, err := NewBoltStorage(boltOptions)
		assert.Nil(err)

		cacheOptions := LogCacheOptions{
			LogStore:     store,
			CacheOnWrite: true,
			TTL:          2 * time.Second,
		}
		cacheStore := NewLogCache(cacheOptions)
		defer func() {
			assert.Nil(os.RemoveAll(getRootDir(boltOptions.DataDir)))
			assert.Nil(cacheStore.Close())
		}()

		var logs []*LogEntry
		for index := range 100 {
			logs = append(logs, &LogEntry{
				Index: uint64(index),
				Term:  1,
			})
		}

		assert.Nil(cacheStore.StoreLogs(logs))

		index := uint64(100)
		_, err = cacheStore.GetLogByIndex(index)
		assert.Error(err)
		assert.Nil(cacheStore.StoreLog(&LogEntry{Index: index, Term: 1}))
		result, err := cacheStore.GetLogByIndex(index)
		assert.Nil(err)
		assert.Equal(index, result.Index)

		time.Sleep(2 * time.Second)
		result, err = cacheStore.GetLogByIndex(index)
		assert.Nil(err)
		assert.Equal(index, result.Index)
	})

	t.Run("cache_get_logs_by_range", func(t *testing.T) {
		boltOptions := BoltOptions{
			DataDir: filepath.Join(os.TempDir(), "rafty_test", "cache_get_logs_by_range"),
			Options: bbolt.DefaultOptions,
		}

		defer func() {
			assert.Nil(os.RemoveAll(getRootDir(boltOptions.DataDir)))
		}()
		store, err := NewBoltStorage(boltOptions)
		assert.Nil(err)

		cacheOptions := LogCacheOptions{
			LogStore:     store,
			CacheOnWrite: true,
			TTL:          2 * time.Second,
		}
		cacheStore := NewLogCache(cacheOptions)
		defer func() {
			assert.Nil(os.RemoveAll(getRootDir(boltOptions.DataDir)))
			assert.Nil(cacheStore.Close())
		}()

		var logs []*LogEntry
		for index := range 100 {
			logs = append(logs, &LogEntry{
				Index: uint64(index),
				Term:  1,
			})
		}

		assert.Nil(cacheStore.StoreLogs(logs))
		min, max, maxAppendEntries := uint64(0), uint64(50), uint64(10)
		response := cacheStore.GetLogsByRange(min, max, maxAppendEntries)
		assert.Nil(response.Err)

		time.Sleep(2 * time.Second)
		response = cacheStore.GetLogsByRange(min, max, maxAppendEntries)
		assert.Greater(response.Total, maxAppendEntries)
		assert.Equal(true, response.SendSnapshot)
	})

	t.Run("cache_get_last_configuration", func(t *testing.T) {
		boltOptions := BoltOptions{
			DataDir: filepath.Join(os.TempDir(), "rafty_test", "cache_get_last_configuration"),
			Options: bbolt.DefaultOptions,
		}

		defer func() {
			assert.Nil(os.RemoveAll(getRootDir(boltOptions.DataDir)))
		}()
		store, err := NewBoltStorage(boltOptions)
		assert.Nil(err)

		cacheOptions := LogCacheOptions{
			LogStore:     store,
			CacheOnWrite: true,
			TTL:          2 * time.Second,
		}
		cacheStore := NewLogCache(cacheOptions)
		defer func() {
			assert.Nil(os.RemoveAll(getRootDir(boltOptions.DataDir)))
			assert.Nil(cacheStore.Close())
		}()

		_, err = cacheStore.GetLastConfiguration()
		assert.Error(err)

		enc := EncodePeers([]Peer{{Address: "127.0.0.1:60000", ID: "60"}, {Address: "127.0.0.1:61000", ID: "61"}, {Address: "127.0.0.1:62000", ID: "62"}})
		configIndex := uint64(50)
		log := &LogEntry{
			LogType: uint32(LogConfiguration),
			Index:   configIndex,
			Term:    1,
			Command: enc,
		}

		assert.Nil(cacheStore.StoreLog(log))
		_, err = cacheStore.GetLastConfiguration()
		assert.Nil(err)

		time.Sleep(2 * time.Second)
		_, err = cacheStore.GetLastConfiguration()
		assert.Nil(err)
	})

	t.Run("cache_discard_logs", func(t *testing.T) {
		boltOptions := BoltOptions{
			DataDir: filepath.Join(os.TempDir(), "rafty_test", "cache_discard_logs"),
			Options: bbolt.DefaultOptions,
		}

		defer func() {
			assert.Nil(os.RemoveAll(getRootDir(boltOptions.DataDir)))
		}()
		store, err := NewBoltStorage(boltOptions)
		assert.Nil(err)

		cacheOptions := LogCacheOptions{
			LogStore:     store,
			CacheOnWrite: true,
		}
		cacheStore := NewLogCache(cacheOptions)
		defer func() {
			assert.Nil(os.RemoveAll(getRootDir(boltOptions.DataDir)))
			assert.Nil(cacheStore.Close())
		}()

		assert.Nil(cacheStore.DiscardLogs(0, 100))
	})

	t.Run("cache_compact_logs", func(t *testing.T) {
		boltOptions := BoltOptions{
			DataDir: filepath.Join(os.TempDir(), "rafty_test", "cache_compact_logs"),
			Options: bbolt.DefaultOptions,
		}

		defer func() {
			assert.Nil(os.RemoveAll(getRootDir(boltOptions.DataDir)))
		}()
		store, err := NewBoltStorage(boltOptions)
		assert.Nil(err)

		cacheOptions := LogCacheOptions{
			LogStore:     store,
			CacheOnWrite: true,
		}
		cacheStore := NewLogCache(cacheOptions)
		defer func() {
			assert.Nil(os.RemoveAll(getRootDir(boltOptions.DataDir)))
			assert.Nil(cacheStore.Close())
		}()

		max := uint64(100)
		key := max - 1
		assert.Nil(cacheStore.CompactLogs(max))

		var logs []*LogEntry
		for index := range max {
			logs = append(logs, &LogEntry{
				Index: index,
				Term:  1,
			})
		}

		assert.Nil(cacheStore.StoreLogs(logs))
		assert.Nil(cacheStore.CompactLogs(key))
		_, err = cacheStore.GetLogByIndex(key)
		assert.Nil(err)
	})

	t.Run("cache_first_index_last_index", func(t *testing.T) {
		boltOptions := BoltOptions{
			DataDir: filepath.Join(os.TempDir(), "rafty_test", "cache_first_index_last_index"),
			Options: bbolt.DefaultOptions,
		}

		defer func() {
			assert.Nil(os.RemoveAll(getRootDir(boltOptions.DataDir)))
		}()
		store, err := NewBoltStorage(boltOptions)
		assert.Nil(err)

		cacheOptions := LogCacheOptions{
			LogStore:     store,
			CacheOnWrite: true,
			TTL:          2 * time.Second,
		}
		cacheStore := NewLogCache(cacheOptions)
		defer func() {
			assert.Nil(os.RemoveAll(getRootDir(boltOptions.DataDir)))
			assert.Nil(cacheStore.Close())
		}()

		var logs []*LogEntry
		for index := range 10 {
			logs = append(logs, &LogEntry{
				Index: uint64(index),
				Term:  1,
			})
		}

		first, last := uint64(0), uint64(9)
		_, err = cacheStore.FirstIndex()
		assert.Error(err)
		_, err = cacheStore.LastIndex()
		assert.Error(err)

		assert.Nil(cacheStore.StoreLogs(logs))

		f, err := cacheStore.FirstIndex()
		assert.Nil(err)
		assert.Equal(first, f)
		l, err := cacheStore.LastIndex()
		assert.Nil(err)
		assert.Equal(last, l)

		time.Sleep(time.Second)
		f, err = cacheStore.FirstIndex()
		assert.Nil(err)
		assert.Equal(first, f)
		l, err = cacheStore.LastIndex()
		assert.Nil(err)
		assert.Equal(last, l)

		time.Sleep(2 * time.Second)
		f, err = cacheStore.FirstIndex()
		assert.Nil(err)
		assert.Equal(first, f)
		l, err = cacheStore.LastIndex()
		assert.Nil(err)
		assert.Equal(last, l)
	})
}
