package main

import (
	"github.com/curtisnewbie/file-service-follower/domain"
	"github.com/curtisnewbie/gocommon/config"
	"github.com/curtisnewbie/gocommon/util"
)

func main() {

	_, conf := config.DefaultParseProfConf()
	if err := config.InitDBFromConfig(&conf.DBConf); err != nil {
		panic(err)
	}
	config.InitRedisFromConfig(&conf.RedisConf)

	// register jobs
	s := util.ScheduleCron("0/5 * * * * *", func() {
		domain.SyncFileInfoEvents()
	})
	s.StartBlocking()
}
