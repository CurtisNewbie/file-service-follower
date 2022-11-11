package main

import (
	"os"

	"github.com/curtisnewbie/file-service-follower/domain"
	"github.com/curtisnewbie/gocommon/config"
	"github.com/curtisnewbie/gocommon/mysql"
	"github.com/curtisnewbie/gocommon/util"
)

func main() {

	// TODO: make this configurable
	// for now, it's by default standalone mode
	mode := domain.AM_STANDALONE

	_, conf := config.DefaultParseProfConf(os.Args)
	if err := mysql.InitDBFromConfig(conf.DBConf); err != nil { panic(err) }

	s := util.ScheduleCron("0/3 * * * * *", func() { domain.SyncFileInfoEvents(mode) })

	s.StartBlocking()
}
