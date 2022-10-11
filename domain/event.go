package domain

import (
	"time"

	"github.com/curtisnewbie/gocommon/config"
	"github.com/curtisnewbie/gocommon/dao"
	"github.com/sirupsen/logrus"
)

// event sync - sync_status
type ESStatus string

const (
	ES_FETCHED ESStatus = "FETCHED"
	ES_ACKED   ESStatus = "ACKED"
)

// Event synchronization and acknowledgement
type EventSync struct {
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

	t := tx.Raw(`select event_id from event_sync 
	order by event_id desc 
	limit 1`).Scan(&eventId)

	if t.Error != nil {
		return nil, t.Error
	}

	return &eventId, nil
}

// Sync file_info events
func SyncFileInfoEvents() {
	lastEventId, e := FindLastEventId()

	if e != nil {
		logrus.Errorf("Failed to find last eventId, %v", e)
		return
	}

	logrus.Infof("Last eventId: %v", *lastEventId)
}
