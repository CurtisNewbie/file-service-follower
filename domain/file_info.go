package domain

import (
	"time"

	"github.com/curtisnewbie/gocommon/dao"
)

type FileInfo struct {
	ID               *int32
	Name             *string
	Uuid             *string
	IsLogicDeleted   *int32
	IsPhysicDeleted  *int32
	SizeInBytes      *int64
	UploaderId       *int32
	UploaderName     *int32
	UploadTime       *time.Time
	LogicDeleteTime  *time.Time
	PhysicDeleteTime *time.Time
	UserGroup        *int32
	FsGroupId        *int32
	FileType         *string
	ParentFile       *string
	CreateTime       *time.Time
	CreateBy         *string
	UpdateTime       *time.Time
	UpdateBy         *string
	IsDel            *dao.IS_DEL
}
