package client

import (
	"fmt"
	"testing"

	"github.com/curtisnewbie/gocommon"
)

func preTest() {
	gocommon.SetProp(PROP_FILE_SERVICE_URL, "http://localhost:8080")
	gocommon.SetProp(PROP_SYNC_SECRET, "123456")
}

func TestDownloadSyncFile(t *testing.T) {
	preTest()
	fileKey := "e2e63cfd-a7fa-4b8a-9cb4-3a6f85991e3b"
	err := DownloadSyncFile(SyncFileInfoReq{FileKey: fileKey}, fmt.Sprintf("/tmp/%s.png", fileKey))
	if err != nil {
		t.Error(err)
	}
}

func TestFetchSyncFileInfo(t *testing.T) {
	preTest()
	r, e := FetchSyncFileInfo(SyncFileInfoReq{FileKey: "e2e63cfd-a7fa-4b8a-9cb4-3a6f85991e3b"})
	if e != nil {
		t.Error(e)
		return
	}
	if r == nil {
		t.Error("Resp is nil")
	}
}

func TestPollEvents(t *testing.T) {
	preTest()
	r, e := PollEvents(PollEventReq{EventId: 0, Limit: 1})
	if e != nil {
		t.Error(e)
		return
	}
	if r == nil {
		t.Error("Resp is nil")
	}
}
