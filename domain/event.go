package domain

import (
	"time"

	"github.com/curtisnewbie/gocommon/config"
	"github.com/curtisnewbie/gocommon/dao"
	"github.com/sirupsen/logrus"
)

// event sync status
type ESStatus string

// event type
type EventType string

const (
	ES_FETCHED ESStatus = "FETCHED"
	ES_ACKED   ESStatus = "ACKED"

	ET_FILE_ADDED   EventType = "FILE_ADDED"
	ET_FILE_DELETED EventType = "FILE_DELETED"
	ET_FILE_UPDATED EventType = "FILE_UPDATED"

	FETCH_LIMIT int = 30
)

type FileEvent struct {
	EventId    *int32
	CreateTime *time.Time
	Type       *EventType
	Data       *FileInfo
}

// Event synchronization and acknowledgement
type FileEventSync struct {
	ID         *int32
	EventId    *int32
	SyncStatus *ESStatus
	FetchTime  *time.Time
	AckTime    *time.Time
	CreateTime *time.Time
	CreateBy   *string
	UpdateTime *time.Time
	UpdateBy   *string
	IsDel      *dao.IS_DEL
}

// find last eventId
func FindLastEventId() (*int32, error) {
	tx := config.GetDB()

	// 0 is the default value, it's also the first eventId used when we don't have any
	var eventId int32

	t := tx.Raw(`select event_id from file_event_sync 
	order by event_id desc 
	limit 1`).Scan(&eventId)

	if t.Error != nil {
		return nil, t.Error
	}

	return &eventId, nil
}

// find last fetched eventId
func FindLastNonAckedEventId() (*int32, error) {
	tx := config.GetDB()

	// 0 is the default value, it's also the first eventId used when we don't have any
	var eventId int32

	t := tx.Raw(`select event_id from file_event_sync 
	where sync_status = ? 
	order by event_id desc 
	limit 1`, ES_FETCHED).Scan(&eventId)

	if t.Error != nil {
		return nil, t.Error
	}

	// return nil if none is found
	if t.RowsAffected < 1 {
		return nil, nil
	}

	return &eventId, nil
}

// verify whether the event is correctly applied, if so, ack it
func ReAckEvent(eventId *int32) error {
	// TODO impl this
	return nil
}

// ack the event
func AckEvent(eventId *int32) error {
	// TODO impl this
	return nil
}

// fetch event detail
func FetchEventDetail(eventId *int32) (*FileEvent, error) {
	// TODO impl this
	return nil, nil
}

// fetch eventIds after the lastEventId
func FetchEventIdsAfter(lastEventId *int32) ([]int32, error) {
	// TODO impl this
	return nil, nil
}

// apply the FileEvent
func ApplyFileEvent(eventId *int32, fileEvent *FileEvent) error {
	// TODO impl this
	return nil
}

// sync file events, events must be sync in order one by one, if any failed, we start it over unless the last event is acked
func SyncFileInfoEvents() {
	/*
		try to find the last eventId that is not acked for whatever reason,
		there should be at most one event that is not acked, we make sure
		that it's correctly applied before we ack it and fetch more events
	*/
	var err error = nil
	lastNonAckedEventId, err := FindLastNonAckedEventId()
	if err != nil {
		logrus.Errorf("Failed to FindLastNonAckedEventId, eventId: %d, %v", *lastNonAckedEventId, err)
		return
	}
	if lastNonAckedEventId != nil {
		// try to verify and re-ack the event
		err = ReAckEvent(lastNonAckedEventId)
		if err != nil {
			logrus.Errorf("Failed to ReAckEvent, eventId: %d, %v", *lastNonAckedEventId, err)
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
	logrus.Infof("Last eventId: %d, tries to fetch more", *lastEventId)

	// keep fetching until we got all of them
	for {

		// Request more events from file-server, file-server may response list of eventIds after the lastEventId we have here
		newEventIds, err := FetchEventIdsAfter(lastEventId)
		if err != nil {
			logrus.Errorf("Failed to FetchEventIdsAfter, lastEventId: %d, %v", *lastEventId, err)
			return
		}

		// no more new events
		if newEventIds == nil || len(newEventIds) < 1 {
			return
		}

		// Based on the list of eventIds we got, we request detail for each of these eventId, and we apply them one by one
		for _, i := range newEventIds {
			var cei int32 = newEventIds[i]
			logrus.Infof("Fetching detail of eventId: %d", cei)
			fileEvent, err := FetchEventDetail(&cei)
			if err != nil {
				logrus.Errorf("Failed to FetchEventDetail, eventId: %d, %v", cei, err)
				return
			}

			if fileEvent != nil {
				// apply the file event, repeatable action
				err = ApplyFileEvent(&cei, fileEvent)
				if err != nil {
					logrus.Errorf("Failed to ApplyFileEvent, eventId: %d, %v", cei, err)
					return
				}

				// the event has been applied, we now ack it
				err = AckEvent(&cei)
				if err != nil {
					logrus.Errorf("Failed to AckEvent, eventId: %d, %v", cei, err)
					return
				}
			}
		}
	}
}
