package main

import (
	"github.com/curtisnewbie/gocommon/web/server"
	"github.com/gin-gonic/gin"

	"github.com/curtisnewbie/gocommon/config"
)

func main() {

	profile, conf := config.DefaultParseProfConf()

	if err := config.InitDBFromConfig(&conf.DBConf); err != nil {
		panic(err)
	}
	config.InitRedisFromConfig(&conf.RedisConf)

	// register jobs
	// s := util.ScheduleCron("0 0/10 * * * *", data.CleanUpDeletedGallery)
	// s.StartAsync()

	isProd := config.IsProd(profile)
	err := server.BootstrapServer(&conf.ServerConf, isProd, func(router *gin.Engine) {
		// TODO
	})
	if err != nil {
		panic(err)
	}

}
