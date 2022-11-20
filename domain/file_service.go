package domain

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"strings"

	"github.com/curtisnewbie/gocommon/common"
	"github.com/sirupsen/logrus"
)

const (
	/*
		--------------------------------

		Event Type

		--------------------------------
	*/

	// EventType: file uploaded
	ET_FILE_ADDED EventType = "UPLOADED"
	// EventType: file deleted
	ET_FILE_DELETED EventType = "DELETED"

	/*
		--------------------------------

		Props

		--------------------------------
	*/
	PROP_SYNC_SECRET      = "client.secret"
	PROP_FILE_SERVICE_URL = "client.fileServiceUrl"

	/*
		--------------------------------

		File Type

		--------------------------------
	*/
	// FileType file
	FT_FILE FileType = "FILE"
	// FileType directory
	FT_DIR FileType = "DIR"
)

var (
	ErrMissingSecret = errors.New("missing secret for event sync")
	ErrMissingUrl    = errors.New("missing client.fileServiceUrl configuration, unable to resolve base url for file-service")
)

// Event type
type EventType string

// File type
type FileType string

// File event
type FileEvent struct {
	EventId int    `json:"eventId"`
	Type    string `json:"type"`
	FileKey string `json:"fileKey"`
}

type PollEventReq struct {
	EventId int    `json:"eventId"`
	Limit   int    `json:"limit"`
	Secret  string `json:"secret"`
}

type PollEventResp struct {
	common.Resp
	Data []FileEvent `json:"data"`
}

type SyncFileInfoReq struct {
	FileKey string `json:"fileKey"`
	Secret  string `json:"secret"`
}

type SyncFileInfoResp struct {
	common.Resp
	Data *FileInfo `json:"data"`
}

type FileInfo struct {
	Name         *string
	Uuid         *string
	SizeInBytes  *int64
	IsDeleted    *bool
	UploaderId   *int
	UploaderName *string
	UserGroup    *int
	FileType     *string
	ParentFile   *string
}

// Fetch info of the file
func DownloadSyncFile(req SyncFileInfoReq, absPath string) error {
	req.Secret = common.GetPropStr(PROP_SYNC_SECRET)
	if req.Secret == "" {
		return ErrMissingSecret
	}

	var e error
	var url string
	if url, e = buildFileServiceUrl("/open/api/sync/file/download"); e != nil {
		return e
	}
	logrus.Infof("Requesting: %s", url)

	var payload []byte
	if payload, e = json.Marshal(req); e != nil {
		return e
	}

	var out *os.File
	if out, e = os.Create(absPath); e != nil {
		return e
	}
	defer out.Close()

	resp, e := http.Post(url, "application/json", bytes.NewBuffer(payload))
	if e != nil {
		return e
	}
	defer resp.Body.Close()

	_, e = io.Copy(out, resp.Body)
	if e != nil {
		return e
	}

	logrus.Infof("Finished downloading file, url: %s", url)
	return nil
}

// Fetch info of the file
func FetchSyncFileInfo(req SyncFileInfoReq) (*SyncFileInfoResp, error) {
	req.Secret = common.GetPropStr(PROP_SYNC_SECRET)
	if req.Secret == "" {
		return nil, ErrMissingSecret
	}

	url, e := buildFileServiceUrl("/open/api/sync/file/info")
	if e != nil {
		return nil, e
	}
	logrus.Infof("Requesting: %s", url)

	payload, e := json.Marshal(req)
	if e != nil {
		return nil, e
	}

	r, e := http.Post(url, "application/json", bytes.NewBuffer(payload))
	if e != nil {
		return nil, e
	}
	defer r.Body.Close()

	body, e := io.ReadAll(r.Body)
	if e != nil {
		return nil, e
	}
	logrus.Infof("FetchSyncFileInfo, resp: %v", string(body))

	var resp SyncFileInfoResp
	if e = json.Unmarshal(body, &resp); e != nil {
		return nil, e
	}

	if resp.Resp.Error {
		return nil, common.NewWebErr(resp.Resp.Msg)
	}
	return &resp, nil
}

// Poll Events
func PollEvents(req PollEventReq) (*PollEventResp, error) {
	req.Secret = common.GetPropStr(PROP_SYNC_SECRET)
	if req.Secret == "" {
		return nil, ErrMissingSecret
	}

	url, e := buildFileServiceUrl("/open/api/sync/event/poll")
	if e != nil {
		return nil, e
	}
	logrus.Infof("Requesting: %s", url)

	payload, e := json.Marshal(req)
	if e != nil {
		return nil, e
	}

	r, e := http.Post(url, "application/json", bytes.NewBuffer(payload))
	if e != nil {
		return nil, e
	}
	defer r.Body.Close()

	body, e := io.ReadAll(r.Body)
	if e != nil {
		return nil, e
	}
	logrus.Infof("Poll events, resp: %v", string(body))

	var resp PollEventResp
	if e = json.Unmarshal(body, &resp); e != nil {
		return nil, e
	}

	if resp.Resp.Error {
		return nil, common.NewWebErr(resp.Resp.Msg)
	}
	return &resp, nil

}

// Concatenate given relative url to base url, the relUrl may or may not start with "/"
func buildFileServiceUrl(relUrl string) (string, error) {
	if !strings.HasPrefix(relUrl, "/") {
		relUrl = "/" + relUrl
	}

	baseUrl := common.GetPropStr(PROP_FILE_SERVICE_URL)
	if baseUrl == "" {
		return "", ErrMissingUrl
	}
	return baseUrl + relUrl, nil
}
