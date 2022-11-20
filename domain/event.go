package domain

import (
	"errors"
	"os"
	"sync"
	"time"

	"github.com/curtisnewbie/gocommon/common"
	"github.com/curtisnewbie/gocommon/redis"
	"github.com/curtisnewbie/gocommon/sqlite"
	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

// event sync status
type ESStatus string

/*
	App mode (server / standalone)

	if server mode is selected, then redis must be deployed for distributed locking

	if standalone mode is selected, then it will simply used mutex lock for syncing
*/
type AppMode string

var (
	syncFileInfoEventsMutex sync.Mutex
)

const (

	/*
		--------------------------------

		Event Sync Status

		--------------------------------
	*/

	// event is fetched, but not acked
	ES_FETCHED ESStatus = "FETCHED"

	// event is fetched and acked
	ES_ACKED ESStatus = "ACKED"

	// limit of eventIds fetched
	FETCH_LIMIT int = 30

	/*
		--------------------------------

		App Mode

		--------------------------------
	*/

	// AppMode: standalone mode
	AM_STANDALONE AppMode = "STANDALONE"

	// AppMode: cluster mode
	AM_CLUSTER AppMode = "CLUSTER"

	PROP_FILE_BASE = "file.base"
)

// file event synchronization and acknowledgement
type FileEventSync struct {
	Id         int
	EventId    int
	FileKey    string
	EventType  string
	SyncStatus ESStatus
	FetchTime  time.Time
	AckTime    time.Time
	CreateTime time.Time
	CreateBy   string
	UpdateTime time.Time
	UpdateBy   string
	IsDel      common.IS_DEL
}

func (fes FileEventSync) isZero() bool {
	return fes.Id < 1
}

// find last eventId, if none is found, 0 is returned
func FindLastEventId() (int, error) {
	tx := sqlite.GetSqlite()

	// 0 is the default value, it's also the first eventId used when we don't have any
	var eventId int

	t := tx.Raw(`
	SELECT event_id FROM file_event_sync 
	ORDER BY event_id DESC 
	LIMIT 1
	`).Scan(&eventId)

	if t.Error != nil {
		return 0, t.Error
	}

	return eventId, nil
}

// Find last fetched eventId, if none is found, a FileEventSync with 'zero value' is returned
func FindLastNonAckedEvent() (FileEventSync, error) {
	tx := sqlite.GetSqlite()

	var fes FileEventSync
	t := tx.Raw(`
	SELECT * FROM file_event_sync 
	WHERE sync_status = ? 
	ORDER BY event_id DESC 
	LIMIT 1
	`, ES_FETCHED).Scan(&fes)

	if t.Error != nil {
		return fes, t.Error
	}
	if t.RowsAffected < 1 {
		return fes, nil
	}

	return fes, nil
}

/*
	Apply and ack event

	This func always tries to apply the event and ack it as if it
	has never been applied before. Even if it found the file on disk,
	the downloaded file may be imcomplete or corrupted, the file is
	always truncated and re-downloaded.
*/
func ApplyAndAckEvent(eventId int, fileKey string, eventType string) error {
	// fetch file info from file-server
	sf, e := FetchSyncFileInfo(SyncFileInfoReq{FileKey: fileKey})
	if e != nil {
		return e
	}

	// the file may have been deleted
	if sf.Data == nil {
		logrus.Infof("File for eventId '%d' has been deleted, resp.data=nil, acking event", eventId)
		return AckEvent(eventId)
	}

	// file deleted
	if eventType == string(ET_FILE_DELETED) {
		if e := deleteFileIfPresent(fileKey); e != nil {
			return e
		}
		return AckEvent(eventId)
	}


	// other events
	if eventType != string(ET_FILE_ADDED) {
		return AckEvent(eventId)
	}

	// handle file added events
	// the file is a directory, ack it directly
	if *sf.Data.FileType == string(FT_DIR) {
		logrus.Infof("File for eventId '%d' is a DIR, acking event", eventId)
		return AckEvent(eventId)
	}

	// we always choose to overwrite the regardless of whether the file exists beforehand
	path := resolveFilePath(fileKey)
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		logrus.Warnf("Failed to close file, path: '%s'", path)
	}

	// download file and ack event
	if err := DownloadSyncFile(SyncFileInfoReq{FileKey: fileKey}, path); err != nil {
		return err
	}
	return AckEvent(eventId)
}

// Delete file if present
func deleteFileIfPresent(fileKey string) error {
	fpath := resolveFilePath(fileKey)

	// file doesn't exist
	if _, err := os.Stat(fpath); errors.Is(err, os.ErrNotExist) {
		return nil
	}

	return os.Remove(fpath)
}

// ack the event
func AckEvent(eventId int) error {
	return sqlite.GetSqlite().Exec("UPDATE file_event_sync SET sync_status = ?", ES_ACKED).Error
}

// fetch eventIds after the lastEventId
func FetchEventIdsAfter(lastEventId int) ([]FileEvent, error) {
	resp, e := PollEvents(PollEventReq{EventId: lastEventId, Limit: 30})
	if e != nil {
		return []FileEvent{}, e
	}
	return resp.Data, e
}

// same as doSyncFileInfoEvents(), except that we wrap it with a lock
func _syncFileInfoEventsInLock(syncFunc func(), mode AppMode) {
	if mode == AM_STANDALONE {
		syncFileInfoEventsMutex.Lock()
		defer syncFileInfoEventsMutex.Unlock()
		syncFunc()
	} else {
		redis.RLockRun("fsf:sync:file", func() any {
			syncFunc()
			return nil
		})
	}
}

// sync file events, events must be sync in order one by one, if any failed, we start it over unless the last event is acked
func SyncFileInfoEvents(mode AppMode) {
	_syncFileInfoEventsInLock(_doSyncFileInfoEvents, mode)
}

// sync file events, events must be sync in order one by one, if any failed, we start it over unless the last event is acked
func _doSyncFileInfoEvents() {
	/*
		try to find the last eventId that is not acked for whatever reason,
		there should be at most one event that is not acked, we make sure
		that it's correctly applied before we ack it and fetch more events
	*/
	var err error = nil
	lastNonAckedEvent, err := FindLastNonAckedEvent()
	if err != nil {
		logrus.Errorf("Failed to FindLastNonAckedEventId, %v", err)
		return
	}
	if !lastNonAckedEvent.isZero() {
		/*
			Try to verify and re-ack the event

			Normally events are acked right after being fetched, if we find a event
			that is not acked, the file downloading may have failed.

			- If the file is not downloaded at all, we try to download it.
			- If a file is found in the dir, it may be corrupted, overwrite it.
		*/
		err = ApplyAndAckEvent(lastNonAckedEvent.EventId, lastNonAckedEvent.FileKey, lastNonAckedEvent.EventType)
		if err != nil {
			logrus.Errorf("Failed to re-ack event, eventId: %d, %v", lastNonAckedEvent.EventId, err)
			return
		}
	} else {
		logrus.Infof("No non-acked eventId found")
	}

	// keep fetching until we got all of them
	for {
		/*
			try to find the last eventId, we use it as an offset to fetch more eventIds after it,
			by default it's 0, and file-server should recognize it.
		*/
		lastEventId, err := FindLastEventId()
		if err != nil {
			logrus.Errorf("Failed to find last eventId, %v", err)
			return
		}
		logrus.Infof("Last eventId: %d, tries to fetch more", lastEventId)

		// request more events from file-server, file-server may response list of eventIds after the lastEventId we have here
		fileEvents, err := FetchEventIdsAfter(lastEventId)
		if err != nil {
			logrus.Errorf("Failed to FetchEventIdsAfter, lastEventId: %d, %v", lastEventId, err)
			return
		}

		// no more new events, break the method
		if len(fileEvents) < 1 {
			break
		}

		// based on the list of eventIds we got, we request detail for each of these eventId, and we apply them one by one
		for _, fe := range fileEvents {
			logrus.Infof("Handling FileEvent, eventId: %d", fe.EventId)

			err = SaveEvent(fe)
			if err != nil {
				logrus.Errorf("Failed to SaveEvent, eventId: %d, %v", fe.EventId, err)
				return
			}

			// apply the file event
			err = ApplyAndAckEvent(fe.EventId, fe.FileKey, fe.Type)
			if err != nil {
				logrus.Errorf("Failed to ApplyFileEvent, eventId: %d, %v", fe.EventId, err)
				return
			}
			logrus.Infof("Successfully applied and acked event, eventId: %d", fe.EventId)
		}
	}
}

// Create schema if absent
func InitSchema() error {
	return sqlite.GetSqlite().Transaction(func(tx *gorm.DB) error {
		if e := tx.Exec(`
			CREATE TABLE IF NOT EXISTS file_event_sync (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				event_id INTEGER NOT NULL DEFAULT '0',
				file_key VARCHAR(64) NOT NULL,
				event_type VARCHAR(25) NOT NULL,
				sync_status VARCHAR(10) NOT NULL DEFAULT 'FETCHED',
				fetch_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
				ack_time TIMESTAMP NULL DEFAULT NULL
			)
		`).Error; e != nil {
			return e
		}

		if e := tx.Exec(`
			CREATE UNIQUE INDEX IF NOT EXISTS event_id_uk ON file_event_sync (event_id)
		`).Error; e != nil {
			return e
		}

		return nil
	})
}

// Resolve file path
func resolveFilePath(fileKey string) string {
	base := common.GetPropStr(PROP_FILE_BASE)
	if base == "" {
		logrus.Fatalf("Unable to resolve base path, missing property: '%s'", PROP_FILE_BASE)
	}
	return base + "/" + fileKey
}

// Save event
func SaveEvent(fe FileEvent) error {
	return sqlite.GetSqlite().Exec(`
		INSERT OR IGNORE INTO file_event_sync (event_id, file_key, event_type, sync_status) VALUES (?, ?, ?, ?)
	`, fe.EventId, fe.FileKey, fe.Type, ES_FETCHED).Error
}
