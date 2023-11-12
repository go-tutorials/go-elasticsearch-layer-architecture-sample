package app

import (
	"context"
	"fmt"
	v "github.com/core-go/core/v10"
	e "github.com/core-go/elasticsearch"
	"github.com/core-go/elasticsearch/query"
	"github.com/core-go/health"
	es "github.com/core-go/health/elasticsearch/v8"
	"github.com/core-go/log"
	"github.com/core-go/search"
	"github.com/elastic/go-elasticsearch/v8"
	"reflect"

	"go-service/internal/handler"
	"go-service/internal/model"
	"go-service/internal/repository"
	"go-service/internal/service"
)

type ApplicationContext struct {
	Health *health.Handler
	User   handler.UserPort
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
		logError(ctx, "Elastic server Error: " + err.Error())
		return nil, err
	}
	fmt.Println("Elastic server response: ", res)

	validator, err := v.NewValidator()
	if err != nil {
		return nil, err
	}

	userType := reflect.TypeOf(model.User{})
	userQueryBuilder := query.NewBuilder(userType)
	userSearchBuilder := e.NewSearchBuilder(client, "users", userType, userQueryBuilder.BuildQuery, search.GetSort)
	userRepository := repository.NewUserRepository(client)
	userService := service.NewUserService(userRepository)
	userHandler := handler.NewUserHandler(userSearchBuilder.Search, userService, validator.Validate, logError)

	elasticSearchChecker := es.NewHealthChecker(client)
	healthHandler := health.NewHandler(elasticSearchChecker)

	return &ApplicationContext{
		Health: healthHandler,
		User:   userHandler,
	}, nil
}
