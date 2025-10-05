package rafty

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/fake"
	"github.com/stretchr/testify/assert"
)

func copyDir(src string, dest string) error {
	if dest[:len(src)] == src {
		return fmt.Errorf("Cannot copy a folder into the folder itself src: %s dest: %s!", src, dest)
	}

	f, err := os.Open(src)
	if err != nil {
		return err
	}

	file, err := f.Stat()
	if err != nil {
		return err
	}
	if !file.IsDir() {
		return fmt.Errorf("Source %s is not a directory!", file.Name())
	}

	err = os.Mkdir(dest, 0755)
	if err != nil {
		return err
	}

	files, err := os.ReadDir(src)
	if err != nil {
		return err
	}

	for _, f := range files {
		source := filepath.Join(src, f.Name())
		destination := filepath.Join(dest, f.Name())
		if f.IsDir() {
			if err = copyDir(source, destination); err != nil {
				return err
			}
		}

		if !f.IsDir() {
			content, err := os.ReadFile(source)
			if err != nil {
				return err
			}

			if err = os.WriteFile(destination, content, 0755); err != nil {
				return err
			}
		}
	}
	return nil
}

func TestSSnapshot(t *testing.T) {
	assert := assert.New(t)

	t.Run("newSnapshot_max_0", func(t *testing.T) {
		dataDir := filepath.Join(os.TempDir(), "rafty_test", fake.CharactersN(20), "newSnapshot_max_0")
		defer func() {
			assert.Nil(os.RemoveAll(dataDir))
		}()
		snapshotConfig := NewSnapshot(dataDir, 0)
		assert.Equal(1, snapshotConfig.maxSnapshots)
	})

	t.Run("newSnapshot_max_1", func(t *testing.T) {
		dataDir := filepath.Join(os.TempDir(), "rafty_test", fake.CharactersN(20), "newSnapshot_max_1")
		defer func() {
			assert.Nil(os.RemoveAll(dataDir))
		}()
		snapshotConfig := NewSnapshot(dataDir, 1)
		assert.Equal(1, snapshotConfig.maxSnapshots)
	})

	t.Run("prepareSnapshotConfig_err_tmp_dir", func(t *testing.T) {
		x := filepath.Join(os.TempDir(), "rafty_test", fake.CharactersN(20))
		assert.Nil(os.MkdirAll(x, 0750))
		dataDir := filepath.Join(x, "prepareSnapshotConfig_err_tmp_dir")
		file, err := os.Create(dataDir)
		assert.Nil(err)
		assert.Nil(file.Close())
		defer func() {
			assert.Nil(os.RemoveAll(x))
		}()

		snapshotConfig := NewSnapshot(dataDir, 0)
		assert.Equal(1, snapshotConfig.maxSnapshots)

		lastIncludedIndex, lastIncludedTerm, lastAppliedConfigIndex, lastAppliedConfigTerm := uint64(1), uint64(1), uint64(1), uint64(1)
		_, err = snapshotConfig.PrepareSnapshotWriter(lastIncludedIndex, lastIncludedTerm, lastAppliedConfigIndex, lastAppliedConfigTerm, Configuration{})
		assert.Error(err)
	})

	t.Run("prepareSnapshotConfig", func(t *testing.T) {
		x := filepath.Join(os.TempDir(), "rafty_test", fake.CharactersN(20))
		dataDir := filepath.Join(x, "prepareSnapshotConfig")
		defer func() {
			assert.Nil(os.RemoveAll(x))
		}()

		snapshotConfig := NewSnapshot(dataDir, 0)
		assert.Equal(1, snapshotConfig.maxSnapshots)

		lastIncludedIndex, lastIncludedTerm, lastAppliedConfigIndex, lastAppliedConfigTerm := uint64(1), uint64(1), uint64(1), uint64(1)
		snapshot, err := snapshotConfig.PrepareSnapshotWriter(lastIncludedIndex, lastIncludedTerm, lastAppliedConfigIndex, lastAppliedConfigTerm, Configuration{})
		assert.Nil(err)

		assert.NotEmpty(snapshot.Name())
		assert.Equal(lastIncludedIndex, snapshot.Metadata().LastIncludedIndex)
		data := []byte("data1\ndata2")

		_, err = snapshot.Write(data)
		assert.Nil(err)
		assert.Nil(snapshot.Close())
		// double close to return err when already synced
		assert.Error(snapshot.Close())
		assert.Greater(snapshot.Metadata().Size, int64(10))
		assert.Error(snapshot.Metadata().file.Close())
	})

	t.Run("prepareSnapshotConfig_removeOldSnapshot", func(t *testing.T) {
		x := filepath.Join(os.TempDir(), "rafty_test", fake.CharactersN(20))
		dataDir := filepath.Join(x, "prepareSnapshotConfig_removeOldSnapshot")
		defer func() {
			assert.Nil(os.RemoveAll(x))
		}()

		for dirIndex := range 2 {
			snapshotConfig := NewSnapshot(dataDir, 0)
			assert.Equal(1, snapshotConfig.maxSnapshots)

			// create the normal snapshot
			var lastIncludedIndex, lastIncludedTerm, lastAppliedConfigIndex, lastAppliedConfigTerm uint64
			lastIncludedIndex, lastIncludedTerm, lastAppliedConfigIndex, lastAppliedConfigTerm = uint64(1), uint64(1), uint64(1), uint64(1)
			snapshot, err := snapshotConfig.PrepareSnapshotWriter(lastIncludedIndex, lastIncludedTerm, lastAppliedConfigIndex, lastAppliedConfigTerm, Configuration{})
			assert.Nil(err)

			assert.NotEmpty(snapshot.Name())
			assert.Equal(lastIncludedIndex, snapshot.Metadata().LastIncludedIndex)
			data := []byte("data1\ndata2")
			_, err = snapshot.Write(data)
			assert.Nil(err)
			assert.Nil(snapshot.Close())
			// double close to return err when already synced
			assert.Error(snapshot.Close())
			assert.Greater(snapshot.Metadata().Size, int64(10))
			assert.Error(snapshot.Metadata().file.Close())
			time.Sleep(100 * time.Millisecond)

			if dirIndex == 0 {
				baseDir := filepath.Dir(snapshotConfig.dataDir)
				// copy snapshot directory
				for i := range 5 {
					switch i {
					case 0:
						continue
					case 1:
						lastIncludedIndex, lastIncludedTerm, lastAppliedConfigIndex, lastAppliedConfigTerm = uint64(1), uint64(2), uint64(1), uint64(1)
					case 2:
						lastIncludedIndex, lastIncludedTerm, lastAppliedConfigIndex, lastAppliedConfigTerm = uint64(1), uint64(3), uint64(1), uint64(1)
					case 3:
						lastIncludedIndex, lastIncludedTerm, lastAppliedConfigIndex, lastAppliedConfigTerm = uint64(1), uint64(4), uint64(1), uint64(1)
					case 4:
						lastIncludedIndex, lastIncludedTerm, lastAppliedConfigIndex, lastAppliedConfigTerm = uint64(2), uint64(1), uint64(1), uint64(1)
					case 5:
						lastIncludedIndex, lastIncludedTerm, lastAppliedConfigIndex, lastAppliedConfigTerm = uint64(3), uint64(2), uint64(1), uint64(1)
					}
					assert.Nil(copyDir(snapshotConfig.dataDir, filepath.Join(baseDir, makeSnapshotName(lastIncludedIndex, lastIncludedTerm))))
				}
			}
		}
	})

	t.Run("prepareSnapshotConfig_removeOldSnapshot_err_readdir", func(t *testing.T) {
		x := filepath.Join(os.TempDir(), "rafty_test", fake.CharactersN(20))
		dataDir := filepath.Join(x, "prepareSnapshotConfig_removeOldSnapshot_err_readdir")
		defer func() {
			assert.Nil(os.RemoveAll(x))
		}()

		snapshotConfig := NewSnapshot(dataDir, 0)
		assert.Equal(1, snapshotConfig.maxSnapshots)

		// create the normal snapshot
		var lastIncludedIndex, lastIncludedTerm, lastAppliedConfigIndex, lastAppliedConfigTerm uint64
		lastIncludedIndex, lastIncludedTerm, lastAppliedConfigIndex, lastAppliedConfigTerm = uint64(1), uint64(1), uint64(1), uint64(1)
		snapshot, err := snapshotConfig.PrepareSnapshotWriter(lastIncludedIndex, lastIncludedTerm, lastAppliedConfigIndex, lastAppliedConfigTerm, Configuration{})
		assert.Nil(err)

		assert.NotEmpty(snapshot.Name())
		assert.Equal(lastIncludedIndex, snapshot.Metadata().LastIncludedIndex)
		data := []byte("data1\ndata2")
		_, err = snapshot.Write(data)
		assert.Nil(err)
		snapshotConfig.parentDir = "x"
		assert.Error(snapshot.Close())
	})

	t.Run("list", func(t *testing.T) {
		x := filepath.Join(os.TempDir(), "rafty_test", fake.CharactersN(20))
		dataDir := filepath.Join(x, "list")
		defer func() {
			assert.Nil(os.RemoveAll(x))
		}()

		snapshotConfig := NewSnapshot(dataDir, 0)
		assert.Equal(1, snapshotConfig.maxSnapshots)

		lastIncludedIndex, lastIncludedTerm, lastAppliedConfigIndex, lastAppliedConfigTerm := uint64(1), uint64(1), uint64(1), uint64(1)
		snapshot, err := snapshotConfig.PrepareSnapshotWriter(lastIncludedIndex, lastIncludedTerm, lastAppliedConfigIndex, lastAppliedConfigTerm, Configuration{})
		assert.Nil(err)

		assert.NotEmpty(snapshot.Name())
		assert.Equal(lastIncludedIndex, snapshot.Metadata().LastIncludedIndex)
		data := []byte("data1\ndata2")
		_, err = snapshot.Write(data)
		assert.Nil(err)
		assert.Nil(snapshot.Close())
		// double close to return err when already synced
		assert.Error(snapshot.Close())
		assert.Greater(snapshot.Metadata().Size, int64(10))
		assert.Error(snapshot.Metadata().file.Close())
		assert.Greater(len(snapshotConfig.List()), 0)
	})

	t.Run("list_err_readdir", func(t *testing.T) {
		x := filepath.Join(os.TempDir(), "rafty_test", fake.CharactersN(20))
		dataDir := filepath.Join(x, "list_err_readdir")
		defer func() {
			assert.Nil(os.RemoveAll(x))
		}()

		snapshotConfig := NewSnapshot(dataDir, 0)
		assert.Equal(1, snapshotConfig.maxSnapshots)

		lastIncludedIndex, lastIncludedTerm, lastAppliedConfigIndex, lastAppliedConfigTerm := uint64(1), uint64(1), uint64(1), uint64(1)
		snapshot, err := snapshotConfig.PrepareSnapshotWriter(lastIncludedIndex, lastIncludedTerm, lastAppliedConfigIndex, lastAppliedConfigTerm, Configuration{})
		assert.Nil(err)

		assert.NotEmpty(snapshot.Name())
		assert.Equal(lastIncludedIndex, snapshot.Metadata().LastIncludedIndex)
		data := []byte("data1\ndata2")
		_, err = snapshot.Write(data)
		assert.Nil(err)
		assert.Nil(snapshot.Close())
		// double close to return err when already synced
		assert.Error(snapshot.Close())
		assert.Greater(snapshot.Metadata().Size, int64(10))
		assert.Error(snapshot.Metadata().file.Close())
		savedParentDir := snapshotConfig.parentDir
		snapshotConfig.parentDir = "x"
		assert.Equal([]*SnapshotMetadata(nil), snapshotConfig.List())
		snapshotConfig.parentDir = savedParentDir
		time.Sleep(200 * time.Millisecond)

		snapshotDataDir := filepath.Join(savedParentDir, makeSnapshotName(lastIncludedIndex, lastIncludedTerm))
		snapshotMetadataFileName := filepath.Join(savedParentDir, makeSnapshotName(lastIncludedIndex, lastIncludedTerm), snapshotMetadataFile)
		assert.Nil(os.MkdirAll(snapshotDataDir, 0750))
		assert.Nil(snapshotConfig.List())
		assert.Nil(os.WriteFile(snapshotMetadataFileName, []byte("a=b"), 0644))
		assert.Nil(snapshotConfig.List())
	})

	t.Run("discard", func(t *testing.T) {
		x := filepath.Join(os.TempDir(), "rafty_test", fake.CharactersN(20))
		dataDir := filepath.Join(x, "discard")
		defer func() {
			assert.Nil(os.RemoveAll(x))
		}()

		snapshotConfig := NewSnapshot(dataDir, 0)
		assert.Equal(1, snapshotConfig.maxSnapshots)

		lastIncludedIndex, lastIncludedTerm, lastAppliedConfigIndex, lastAppliedConfigTerm := uint64(1), uint64(1), uint64(1), uint64(1)
		snapshot, err := snapshotConfig.PrepareSnapshotWriter(lastIncludedIndex, lastIncludedTerm, lastAppliedConfigIndex, lastAppliedConfigTerm, Configuration{})
		assert.Nil(err)

		assert.NotEmpty(snapshot.Name())
		assert.Equal(lastIncludedIndex, snapshot.Metadata().LastIncludedIndex)
		data := []byte("data1\ndata2")
		_, err = snapshot.Write(data)
		assert.Nil(err)
		assert.Nil(snapshot.Discard())
		// double to return nil
		assert.Nil(snapshot.Discard())
	})

	t.Run("reader", func(t *testing.T) {
		x := filepath.Join(os.TempDir(), "rafty_test", fake.CharactersN(20))
		dataDir := filepath.Join(x, "reader")
		defer func() {
			assert.Nil(os.RemoveAll(x))
		}()

		snapshotConfig := NewSnapshot(dataDir, 0)
		assert.Equal(1, snapshotConfig.maxSnapshots)

		lastIncludedIndex, lastIncludedTerm, lastAppliedConfigIndex, lastAppliedConfigTerm := uint64(1), uint64(1), uint64(1), uint64(1)
		snapshot, err := snapshotConfig.PrepareSnapshotWriter(lastIncludedIndex, lastIncludedTerm, lastAppliedConfigIndex, lastAppliedConfigTerm, Configuration{})
		assert.Nil(err)

		assert.NotEmpty(snapshot.Name())
		assert.Equal(lastIncludedIndex, snapshot.Metadata().LastIncludedIndex)
		assert.Equal(lastIncludedTerm, snapshot.Metadata().LastIncludedTerm)
		data := []byte("data1\ndata2")
		_, err = snapshot.Write(data)
		assert.Nil(err)
		assert.Nil(snapshot.Close())

		snapshots := snapshotConfig.List()
		last := snapshots[0]
		reader, _, err := snapshotConfig.PrepareSnapshotReader(last.SnapshotName)
		assert.Nil(err)
		assert.Equal(lastIncludedIndex, reader.Metadata().LastIncludedIndex)
		assert.Equal(lastIncludedTerm, reader.Metadata().LastIncludedTerm)
	})

	t.Run("reader_error", func(t *testing.T) {
		x := filepath.Join(os.TempDir(), "rafty_test", fake.CharactersN(20))
		dataDir := filepath.Join(x, "reader_error")
		defer func() {
			assert.Nil(os.RemoveAll(x))
		}()

		snapshotConfig := NewSnapshot(dataDir, 0)
		reader, _, err := snapshotConfig.PrepareSnapshotReader("xyz")
		assert.Error(err)
		assert.Nil(reader)
	})

	t.Run("readMetadata_error_read_metadata", func(t *testing.T) {
		x := filepath.Join(os.TempDir(), "rafty_test", fake.CharactersN(20))
		assert.Nil(os.MkdirAll(x, 0750))
		dataDir := filepath.Join(x, "readMetadata_error_read_metadata")
		file, err := os.Create(dataDir)
		assert.Nil(err)
		assert.Nil(file.Close())
		defer func() {
			assert.Nil(os.RemoveAll(x))
		}()

		snapshotConfig := NewSnapshot(x, 0)
		_, err = snapshotConfig.readMetadata(file)
		assert.Error(err)
	})

	t.Run("readMetadata_error_unmarshal", func(t *testing.T) {
		x := filepath.Join(os.TempDir(), "rafty_test", fake.CharactersN(20))
		assert.Nil(os.MkdirAll(x, 0750))
		dataDir := filepath.Join(x, "readMetadata_error_unmarshal")
		file, err := os.Create(dataDir)
		assert.Nil(err)
		_, err = file.Write([]byte("a=b"))
		assert.Nil(err)
		defer func() {
			assert.Nil(file.Close())
			assert.Nil(os.RemoveAll(x))
		}()

		snapshotConfig := NewSnapshot(x, 0)
		reader, err := snapshotConfig.readMetadata(file)
		assert.Error(err)
		assert.Nil(reader)
	})

	t.Run("readMetadata_success", func(t *testing.T) {
		x := filepath.Join(os.TempDir(), "rafty_test", fake.CharactersN(20))
		assert.Nil(os.MkdirAll(x, 0750))
		dataDir := filepath.Join(x, "readMetadata_success")
		file, err := os.Create(dataDir)
		assert.Nil(err)
		_, err = file.Write([]byte(`{"lastIncludedIndex":1}`))
		assert.Nil(file.Sync())
		assert.Nil(err)
		_, err = file.Seek(0, 0)
		assert.Nil(err)
		defer func() {
			assert.Nil(file.Close())
			assert.Nil(os.RemoveAll(x))
		}()

		snapshotConfig := NewSnapshot(x, 0)
		_, err = snapshotConfig.readMetadata(file)
		assert.Nil(err)
	})

	t.Run("close_nil", func(t *testing.T) {
		cfg := &SnapshotConfig{tmpDir: "tmp", dataDir: "data", parentDir: t.TempDir(), maxSnapshots: 1}
		meta := &SnapshotMetadata{}
		sm := &SnapshotManager{
			config:   cfg,
			metadata: meta,
			file:     nil,
		}
		assert.Nil(sm.Close())
	})

	t.Run("backup_restore", func(t *testing.T) {
		s := basicNodeSetup()
		r := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(r.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
			assert.Nil(os.RemoveAll(getRootDir(r.options.DataDir)))
		}()
		s.currentTerm.Store(1)

		max := 100
		for index := range max {
			entry := makeNewLogEntry(s.currentTerm.Load(), LogReplication, []byte(fmt.Sprintf("%s=%d", fake.WordsN(5), index)))
			logs := []*LogEntry{entry}
			s.storeLogs(logs)
			assert.Nil(s.applyConfigEntry(makeProtobufLogEntry(entry)[0]))
		}

		_, err := s.takeSnapshot()
		assert.Nil(err)

		sdatadir := filepath.Join(s.options.DataDir, snapshotDir)
		rdatadir := filepath.Join(r.options.DataDir, snapshotDir)
		assert.Nil(os.MkdirAll(r.options.DataDir, 0750))
		assert.Nil(copyDir(sdatadir, rdatadir))
		snapshotConfig := NewSnapshot(r.options.DataDir, 0)
		snapshots := snapshotConfig.List()
		last := snapshots[0]

		_, file, err := snapshotConfig.PrepareSnapshotReader(last.SnapshotName)
		assert.Nil(err)

		assert.Nil(r.fsm.Restore(file))
		assert.Nil(file.Close())

		lastIndex, err := r.logStore.LastIndex()
		assert.Nil(err)
		assert.Equal(uint64(max), lastIndex)
	})
}
