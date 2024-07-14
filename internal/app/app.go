package app

import (
	"context"
	"fmt"

	"github.com/core-go/health"
	es "github.com/core-go/health/elasticsearch/v8"
	"github.com/core-go/log/zap"
	"github.com/elastic/go-elasticsearch/v8"

	"go-service/internal/user"
)

type ApplicationContext struct {
	Health *health.Handler
	User   user.UserTransport
}

func NewApp(ctx context.Context, config Config) (*ApplicationContext, error) {
	log.Initialize(config.Log)
	logError := log.LogError

	cfg := elasticsearch.Config{Addresses: []string{config.ElasticSearch.Url}}

	client, err := elasticsearch.NewClient(cfg)
	if err != nil {
		logError(ctx, "Cannot connect to elasticSearch. Error: "+err.Error())
		return nil, err
	}

	res, err := client.Info()
	if err != nil {
		logError(ctx, "Elastic server Error: "+err.Error())
		return nil, err
	}
	fmt.Println("Elastic server response: ", res)

	userHandler, err := user.NewUserHandler(client, logError)
	if err != nil {
		return nil, err
	}

	elasticSearchChecker := es.NewHealthChecker(client)
	healthHandler := health.NewHandler(elasticSearchChecker)

	return &ApplicationContext{
		Health: healthHandler,
		User:   userHandler,
	}, nil
}
