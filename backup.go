package gorocksdb

// #include <stdlib.h>
// #include "rocksdb/c.h"
import "C"
import "unsafe"

// BackupEngineInfo represents the information about the backups
// in a backup engine instance. Use this to get the state of the
// backup like number of backups and their ids and timestamps etc.
type BackupEngineInfo struct {
	c *C.rocksdb_backup_engine_info_t
}

// GetCount gets the number backsup available.
func (b *BackupEngineInfo) GetCount() int {
	return int(C.rocksdb_backup_engine_info_count(b.c))
}

// GetTimestamp gets the timestamp at which the backup index was taken.
func (b *BackupEngineInfo) GetTimestamp(index int) int64 {
	return int64(C.rocksdb_backup_engine_info_timestamp(b.c, C.int(index)))
}

// GetBackupID gets an id that uniquely identifies a backup
// regardless of its position.
func (b *BackupEngineInfo) GetBackupID(index int) int64 {
	return int64(C.rocksdb_backup_engine_info_backup_id(b.c, C.int(index)))
}

// GetSize get the size of the backup in bytes.
func (b *BackupEngineInfo) GetSize(index int) int64 {
	return int64(C.rocksdb_backup_engine_info_size(b.c, C.int(index)))
}

// GetNumFiles gets the number of files in the backup index.
func (b *BackupEngineInfo) GetNumFiles(index int) uint32 {
	return uint32(C.rocksdb_backup_engine_info_number_files(b.c, C.int(index)))
}

// Release destroys the backup engine info instance.
func (b *BackupEngineInfo) Release() {
	C.rocksdb_backup_engine_info_destroy(b.c)
	b.c = nil
}

// RestoreOptions captures the options to be used during
// restoration of a backup.
type RestoreOptions struct {
	c *C.rocksdb_restore_options_t
}

// NewRestoreOptions creates a RestoreOptions instance.
func NewRestoreOptions() *RestoreOptions {
	return &RestoreOptions{
		c: C.rocksdb_restore_options_create(),
	}
}

// SetKeepLogFiles is used to set or unset the keep_log_files option
// If true, restore won't overwrite the existing log files in wal_dir. It will
// also move all log files from archive directory to wal_dir.
// By default, this is false.
func (o *RestoreOptions) SetKeepLogFiles(v int) {
	C.rocksdb_restore_options_set_keep_log_files(o.c, C.int(v))
}

// Release destroys this RestoreOptions instance.
func (o *RestoreOptions) Release() {
	C.rocksdb_restore_options_destroy(o.c)
}

// BackupEngine is a reusable handle to a RocksDB Backup, created by
// OpenBackupEngine.
type BackupEngine struct {
	c    *C.rocksdb_backup_engine_t
	path string
	opts *Options
}

// OpenBackupEngine opens a backup engine with specified options.
func OpenBackupEngine(opts *Options, path string) (*BackupEngine, error) {
	var cErr *C.char
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))

	be := C.rocksdb_backup_engine_open(opts.c, cpath, &cErr)
	if cErr != nil {
		return nil, convertErr(cErr)
	}
	return &BackupEngine{
		c:    be,
		path: path,
		opts: opts,
	}, nil
}

// CreateNewBackup takes a new backup from db.
func (b *BackupEngine) CreateNewBackup(db *DB) error {
	var cErr *C.char
	C.rocksdb_backup_engine_create_new_backup(b.c, db.c, &cErr)
	return convertErr(cErr)
}

// GetInfo gets an object that gives information about
// the backups that have already been taken
func (b *BackupEngine) GetInfo() *BackupEngineInfo {
	return &BackupEngineInfo{
		c: C.rocksdb_backup_engine_get_backup_info(b.c),
	}
}

// RestoreDBFromLatestBackup restores the latest backup to dbDir. walDir
// is where the write ahead logs are restored to and usually the same as dbDir.
func (b *BackupEngine) RestoreDBFromLatestBackup(dbDir, walDir string, ro *RestoreOptions) error {
	var cErr *C.char
	cDBDir := C.CString(dbDir)
	cWalDir := C.CString(walDir)
	defer func() {
		C.free(unsafe.Pointer(cDBDir))
		C.free(unsafe.Pointer(cWalDir))
	}()

	C.rocksdb_backup_engine_restore_db_from_latest_backup(b.c, cDBDir, cWalDir, ro.c, &cErr)
	return convertErr(cErr)
}

// PurgeOldBackups purges all but the last num backups.
func (b *BackupEngine) PurgeOldBackups(num uint32) error {
	var cErr *C.char
	C.rocksdb_backup_engine_purge_old_backups(b.c, C.uint32_t(num), &cErr)
	return convertErr(cErr)
}

// Release close the backup engine and cleans up state
// The backups already taken remain on storage.
func (b *BackupEngine) Release() {
	C.rocksdb_backup_engine_close(b.c)
	b.c = nil
}
