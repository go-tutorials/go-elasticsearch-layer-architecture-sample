package user

import (
	"context"
	"net/http"
	"reflect"

	v "github.com/core-go/core/v10"
	"github.com/core-go/search"
	"github.com/core-go/search/elasticsearch"
	"github.com/elastic/go-elasticsearch/v8"

	"go-service/internal/user/handler"
	"go-service/internal/user/model"
	"go-service/internal/user/repository"
	"go-service/internal/user/service"
	e "go-service/pkg/elasticsearch"
)

type UserTransport interface {
	All(w http.ResponseWriter, r *http.Request)
	Search(w http.ResponseWriter, r *http.Request)
	Load(w http.ResponseWriter, r *http.Request)
	Create(w http.ResponseWriter, r *http.Request)
	Update(w http.ResponseWriter, r *http.Request)
	Patch(w http.ResponseWriter, r *http.Request)
	Delete(w http.ResponseWriter, r *http.Request)
}

func NewUserHandler(client *elasticsearch.Client, logError func(context.Context, string, ...map[string]interface{})) (UserTransport, error) {
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
	return userHandler, nil
}
