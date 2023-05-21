package app

import (
	"github.com/core-go/core"
	"github.com/core-go/log"
	mid "github.com/core-go/log/middleware"
)

type Config struct {
	Server        core.ServerConf     `mapstructure:"server"`
	ElasticSearch ElasticSearchConfig `mapstructure:"elastic_search"`
	Log           log.Config          `mapstructure:"log"`
	MiddleWare    mid.LogConfig       `mapstructure:"middleware"`
}

type ElasticSearchConfig struct {
	Url      string `mapstructure:"url"`
	Username string `mapstructure:"username"`
	Password string `mapstructure:"password"`
}
