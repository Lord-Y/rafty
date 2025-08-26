package rafty

import (
	"bytes"
	"fmt"
	"path/filepath"

	bolt "go.etcd.io/bbolt"
)

const (
	// dbFileName is the name of the database file
	dbFileName string = "rafty.db"
	// bucketLogsName will be used to store rafty logs
	bucketLogsName string = "rafty_logs"
	// bucketMetadataName will be used to store rafty metadata
	bucketMetadataName string = "rafty_metadata"
	// bucketKVName will be used as a simple key/value store
	bucketKVName string = "rafty_kv"
)

type BoltOptions struct {
	// DataDir is the default data directory that will be used to store all data on the disk. It's required
	DataDir string

	// Options hold all bolt options
	Options *bolt.Options
}

type BoltStore struct {
	// dataDir is the default data directory that will be used to store all data on the disk
	// Defaults to os.TempDir()/rafty/db/ ex: /tmp/rafty/db/
	dataDir string

	// db allows us to manipulate the k/v database
	db *bolt.DB
}

func NewBoltStorage(options BoltOptions) (*BoltStore, error) {
	var (
		db  *bolt.DB
		err error
	)
	if options.DataDir == "" {
		return nil, ErrDataDirRequired
	}
	dbdir := filepath.Join(options.DataDir, "db")
	if err := createDirectoryIfNotExist(dbdir, 0750); err != nil {
		return nil, fmt.Errorf("fail to create directory %s: %w", dbdir, err)
	}
	if db, err = bolt.Open(filepath.Join(dbdir, dbFileName), 0600, options.Options); err != nil {
		return nil, err
	}

	store := &BoltStore{
		dataDir: options.DataDir,
		db:      db,
	}

	if !options.Options.ReadOnly {
		if err := store.initializeBuckets(); err != nil {
			return nil, err
		}
	}
	return store, nil
}

// initializeBuckets will initialize all buckets
// required by rafty
func (b *BoltStore) initializeBuckets() error {
	tx, err := b.db.Begin(true)
	if err != nil {
		return err
	}
	defer func() {
		if err := tx.Rollback(); err != nil {
			b.db.Logger().Errorf("Rollback failed: %w", err)
		}
	}()

	if _, err := tx.CreateBucketIfNotExists([]byte(bucketLogsName)); err != nil {
		return err
	}

	if _, err := tx.CreateBucketIfNotExists([]byte(bucketMetadataName)); err != nil {
		return err
	}

	if _, err := tx.CreateBucketIfNotExists([]byte(bucketKVName)); err != nil {
		return err
	}
	return tx.Commit()
}

// Close will close bolt database
func (b *BoltStore) Close() error {
	return b.db.Close()
}

// StoreLogs stores multiple log entries
func (b *BoltStore) StoreLogs(logs []*logEntry) error {
	tx, err := b.db.Begin(true)
	if err != nil {
		return err
	}
	defer func() {
		if err := tx.Rollback(); err != nil {
			b.db.Logger().Errorf("Rollback failed: %w", err)
		}
	}()
	bucket := tx.Bucket([]byte(bucketLogsName))

	for _, log := range logs {
		key := encodeUint64ToBytes(log.Index)
		value := new(bytes.Buffer)
		if err := MarshalBinary(log, value); err != nil {
			return err
		}

		if err = bucket.Put(key, value.Bytes()); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// StoreLog stores a single log entry
func (b *BoltStore) StoreLog(log *logEntry) error {
	return b.StoreLogs([]*logEntry{log})
}

// GetLogByIndex permits to retrieve log from specified index
func (b *BoltStore) GetLogByIndex(index uint64) (*logEntry, error) {
	var log logEntry
	err := b.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(bucketLogsName))
		value := bucket.Get(encodeUint64ToBytes(index))
		if value == nil {
			return ErrLogNotFound
		}

		entry, err := UnmarshalBinary(value)
		if err != nil {
			return err
		}
		log = logEntry{
			FileFormat: uint8(entry.FileFormat),
			Tombstone:  uint8(entry.Tombstone),
			LogType:    uint8(entry.LogType),
			Timestamp:  entry.Timestamp,
			Term:       entry.Term,
			Index:      entry.Index,
			Command:    entry.Command,
		}
		return nil
	})
	return &log, err
}

// GetLogsByRange will return a slice of logs
// with peer lastLogIndex and leader lastLogIndex capped
// by options.MaxAppendEntries
func (b *BoltStore) GetLogsByRange(minIndex, maxIndex, maxAppendEntries uint64) (response GetLogsByRangeResponse) {
	response.Err = b.db.View(func(tx *bolt.Tx) error {
		cursor := tx.Bucket([]byte(bucketLogsName)).Cursor()
		for k, v := cursor.Seek(encodeUint64ToBytes(minIndex)); k != nil; k, v = cursor.Next() {
			if decodeUint64ToBytes(k) > maxIndex {
				return nil
			}
			entry, err := UnmarshalBinary(v)
			if err != nil {
				return err
			}
			response.Logs = append(response.Logs, &logEntry{
				FileFormat: uint8(entry.FileFormat),
				Tombstone:  uint8(entry.Tombstone),
				LogType:    uint8(entry.LogType),
				Timestamp:  entry.Timestamp,
				Term:       entry.Term,
				Index:      entry.Index,
				Command:    entry.Command,
			})
			response.Total += 1
			response.LastLogIndex = entry.Index
			response.LastLogTerm = entry.Term
			if response.Total+1 > maxAppendEntries {
				response.SendSnapshot = true
				return nil
			}
		}
		return ErrLogNotFound
	})
	return
}

// GetLastConfiguration returns the last configuration found
// in logs
func (b *BoltStore) GetLastConfiguration() (*logEntry, error) {
	var log logEntry
	err := b.db.View(func(tx *bolt.Tx) error {
		cursor := tx.Bucket([]byte(bucketLogsName)).Cursor()
		for k, v := cursor.Last(); k != nil; k, v = cursor.Prev() {
			entry, err := UnmarshalBinary(v)
			if err != nil {
				return err
			}
			if logKind(entry.LogType) == logConfiguration {
				log = logEntry{
					FileFormat: uint8(entry.FileFormat),
					Tombstone:  uint8(entry.Tombstone),
					LogType:    uint8(entry.LogType),
					Timestamp:  entry.Timestamp,
					Term:       entry.Term,
					Index:      entry.Index,
					Command:    entry.Command,
				}
				return nil
			}
		}
		return ErrLogNotFound
	})
	return &log, err
}

// DiscardLogs permits to wipe entries with the provided range indexes
func (b *BoltStore) DiscardLogs(minIndex, maxIndex uint64) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(bucketLogsName))
		cursor := bucket.Cursor()

		for k, _ := cursor.Seek(encodeUint64ToBytes(minIndex)); k != nil; k, _ = cursor.Next() {
			if decodeUint64ToBytes(k) > maxIndex {
				return nil
			}
			if err := bucket.Delete(k); err != nil {
				return err
			}
		}
		return nil
	})
}

// GetMetadata will fetch rafty metadata from the k/v store
func (b *BoltStore) GetMetadata() ([]byte, error) {
	return b.getKV(bucketMetadataName, []byte("metadata"))
}

// storeMetadata will store rafty metadata into the k/v bucket
func (b *BoltStore) storeMetadata(value []byte) error {
	return b.storeKV(bucketMetadataName, []byte("metadata"), value)
}

// storeKV is an internal func that allows us store k/v into the specified
// bucket
func (b *BoltStore) storeKV(bucketName string, key, value []byte) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(bucketName))
		return bucket.Put([]byte(key), []byte(value))
	})
}

// getKV is an internal func that allows us to fetch keys from specified
// bucket
func (b *BoltStore) getKV(bucketName string, key []byte) ([]byte, error) {
	var value []byte
	err := b.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(bucketName))
		if value = bucket.Get(key); value == nil {
			return ErrKeyNotFound
		}
		return nil
	})
	return value, err
}

// Set will add key/value to the k/v store.
// An error will be returned if necessary
func (b *BoltStore) Set(key, value []byte) error {
	return b.storeKV(bucketKVName, key, value)
}

// Get will fetch provided key from the k/v store.
// An error will be returned if the key is not found
func (b *BoltStore) Get(key []byte) ([]byte, error) {
	return b.getKV(bucketKVName, key)
}

// SetUint64 will add key/value to the k/v store.
// An error will be returned if necessary
func (b *BoltStore) SetUint64(key, value []byte) error {
	return b.storeKV(bucketKVName, key, value)
}

// GetUint64 will fetch provided key from the k/v store.
// An error will be returned if the key is not found
func (b *BoltStore) GetUint64(key []byte) uint64 {
	value, err := b.getKV(bucketKVName, key)
	if err != nil {
		return 0
	}
	return decodeUint64ToBytes(value)
}

// FistIndex will return the first index from the raft log
func (b *BoltStore) FirstIndex() (uint64, error) {
	var key []byte
	if err := b.db.View(func(tx *bolt.Tx) error {
		cursor := tx.Bucket([]byte(bucketLogsName)).Cursor()
		key, _ = cursor.First()
		return nil
	}); err != nil {
		return 0, ErrStoreClosed
	}
	if len(key) == 0 {
		return 0, ErrKeyNotFound
	}
	return decodeUint64ToBytes(key), nil
}

// LastIndex will return the last index from the raft log
func (b *BoltStore) LastIndex() (uint64, error) {
	var key []byte
	if err := b.db.View(func(tx *bolt.Tx) error {
		cursor := tx.Bucket([]byte(bucketLogsName)).Cursor()
		key, _ = cursor.Last()
		return nil
	}); err != nil {
		return 0, ErrStoreClosed
	}
	if len(key) == 0 {
		return 0, ErrKeyNotFound
	}
	return decodeUint64ToBytes(key), nil
}
