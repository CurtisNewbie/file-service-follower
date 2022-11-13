package domain

import (
	"sync"
	"time"

	"github.com/curtisnewbie/file-service-follower/client"
	"github.com/curtisnewbie/gocommon"
	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

// event sync status
type ESStatus string

// app mode (server / standalone)
//
// if server mode is selected, then redis must be deployed for distributed locking
//
// if standalone mode is selected, then it will simply used mutex lock for syncing
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
)

// file event synchronization and acknowledgement
type FileEventSync struct {
	Id         int
	EventId    int
	FileKey    string
	SyncStatus ESStatus
	FetchTime  time.Time
	AckTime    time.Time
	CreateTime time.Time
	CreateBy   string
	UpdateTime time.Time
	UpdateBy   string
	IsDel      gocommon.IS_DEL
}

func (fes FileEventSync) isZero() bool {
	return fes.Id < 1
}

// find last eventId, if none is found, 0 is returned
func FindLastEventId() (int, error) {
	tx := gocommon.GetSqlite()

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

// find last fetched eventId, if none is found, -1is returned
func FindLastNonAckedEventId() (int, error) {
	tx := gocommon.GetSqlite()

	eventId := -1

	t := tx.Raw(`
	SELECT event_id FROM file_event_sync 
	WHERE sync_status = ? 
	ORDER BY event_id DESC 
	LIMIT 1
	`, ES_FETCHED).Scan(&eventId)

	if t.Error != nil {
		return -1, t.Error
	}
	if t.RowsAffected < 1 {
		return -1, nil
	}

	return eventId, nil
}

// verify whether the event is correctly applied, if so, ack it
func ReAckEvent(eventId int) error {
	// TODO impl this
	return nil
}

// ack the event
func AckEvent(fileEvent client.FileEvent) error {
	// TODO impl this
	return nil
}

// fetch eventIds after the lastEventId
func FetchEventIdsAfter(lastEventId int) ([]client.FileEvent, error) {

	// TODO impl this
	return nil, nil
}

// apply the FileEvent
func ApplyFileEvent(fileEvent client.FileEvent) error {
	// TODO impl this
	return nil
}

// same as doSyncFileInfoEvents(), except that we wrap it with a lock
func _syncFileInfoEventsInLock(syncFunc func(), mode AppMode) {
	if mode == AM_STANDALONE {
		syncFileInfoEventsMutex.Lock()
		defer syncFileInfoEventsMutex.Unlock()
		syncFunc()
	} else {
		gocommon.LockRun("fsf:sync:file", func() any {
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
	lastNonAckedEventId, err := FindLastNonAckedEventId()
	if err != nil {
		logrus.Errorf("Failed to FindLastNonAckedEventId, %v", err)
		return
	}
	if lastNonAckedEventId > -1 {
		// try to verify and re-ack the event
		err = ReAckEvent(lastNonAckedEventId)
		if err != nil {
			logrus.Errorf("Failed to ReAckEvent, eventId: %d, %v", lastNonAckedEventId, err)
			return
		}
	} else {
		logrus.Infof("No non-acked eventId found")
	}

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

	// keep fetching until we got all of them
	for {

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

			// apply the file event, repeatable action
			err = ApplyFileEvent(fe)
			if err != nil {
				logrus.Errorf("Failed to ApplyFileEvent, eventId: %d, %v", fe.EventId, err)
				return
			}

			// the event has been applied, we now ack it, repeatable action
			err = AckEvent(fe)
			if err != nil {
				logrus.Errorf("Failed to AckEvent, eventId: %d, %v", fe.EventId, err)
				return
			}
		}
	}
}

// Create schema if absent
func InitSchema() error {
	return gocommon.GetSqlite().Transaction(func(tx *gorm.DB) error {
		if e := tx.Exec(`
		CREATE TABLE IF NOT EXISTS file_event_sync (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			event_id INTEGER NOT NULL DEFAULT '0',
			file_key VARCHAR(64) NOT NULL,
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
