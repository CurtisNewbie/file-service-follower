package domain

import (
	"testing"

	"github.com/curtisnewbie/gocommon/common"
)

func TestDeleteFileIfPresent(t *testing.T) {
	common.SetProp(PROP_FILE_BASE, "../fsf-base")
	e := deleteFileIfPresent("abc")
	if e != nil {
		t.Error(e)
	}
}