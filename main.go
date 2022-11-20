package main

import (
	"os"

	"github.com/curtisnewbie/file-service-follower/domain"
	"github.com/curtisnewbie/gocommon/common"
)

func main() {
	// TODO: make this configurable
	// for now, it's by default standalone mode
	mode := domain.AM_STANDALONE

	common.DefaultReadConfig(os.Args)
	if e := domain.InitSchema(); e != nil {
		panic(e)
	}

	s := common.ScheduleCron("0/5 * * * * *", func() { domain.SyncFileInfoEvents(mode) })
	s.StartBlocking()
}
