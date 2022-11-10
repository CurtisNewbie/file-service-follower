package main

import (
	"os"

	"github.com/curtisnewbie/file-service-follower/domain"
	"github.com/curtisnewbie/gocommon/config"
	"github.com/curtisnewbie/gocommon/mysql"
	"github.com/curtisnewbie/gocommon/redis"
	"github.com/curtisnewbie/gocommon/util"
)

func main() {

	_, conf := config.DefaultParseProfConf(os.Args)

	if err := mysql.InitDBFromConfig(conf.DBConf); err != nil {
		panic(err)
	}

	redis.InitRedisFromConfig(conf.RedisConf)

	// register jobs
	s := util.ScheduleCron("0/3 * * * * *", func() {
		domain.SyncFileInfoEvents()
	})
	s.StartBlocking()
}
