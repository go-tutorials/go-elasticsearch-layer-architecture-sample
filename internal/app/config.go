package app

import (
	"github.com/core-go/core/server"
	mid "github.com/core-go/log/middleware"
	"github.com/core-go/log/zap"
)

type Config struct {
	Server        server.ServerConf   `mapstructure:"server"`
	ElasticSearch ElasticSearchConfig `mapstructure:"elastic_search"`
	Log           log.Config          `mapstructure:"log"`
	MiddleWare    mid.LogConfig       `mapstructure:"middleware"`
}

type ElasticSearchConfig struct {
	Url      string `mapstructure:"url"`
	Username string `mapstructure:"username"`
	Password string `mapstructure:"password"`
}
