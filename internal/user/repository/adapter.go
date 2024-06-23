package repository

import (
	"context"
	"reflect"

	"github.com/elastic/go-elasticsearch/v8"

	"go-service/internal/user/model"
	es "go-service/pkg/elasticsearch"
)

type UserAdapter struct {
	Client    *elasticsearch.Client
	IndexName string
	idIndex   int
	idJson    string
}

func NewUserRepository(client *elasticsearch.Client) *UserAdapter {
	userType := reflect.TypeOf(model.User{})
	idIndex, _, idJson := es.FindIdField(userType)
	return &UserAdapter{Client: client, IndexName: "users", idIndex: idIndex, idJson: idJson}
}

func (e *UserAdapter) All(ctx context.Context) ([]model.User, error) {
	var users []model.User
	query := make(map[string]interface{})
	err := es.Find(ctx, e.Client, []string{"users"}, query, &users)
	return users, err
}

func (e *UserAdapter) Load(ctx context.Context, id string) (*model.User, error) {
	var user model.User
	ok, err := es.FindOne(ctx, e.Client, "users", id, &user)
	if !ok || err != nil {
		return nil, err
	}
	return &user, nil
}

func (e *UserAdapter) Create(ctx context.Context, user *model.User) (int64, error) {
	id := user.Id
	user.Id = ""
	res, err := es.Create(ctx, e.Client, "users", user, &id)
	user.Id = id
	return res, err
}

func (e *UserAdapter) Update(ctx context.Context, user *model.User) (int64, error) {
	id := user.Id
	user.Id = ""
	res, err := es.Update(ctx, e.Client, "users", user, user.Id)
	user.Id = id
	return res, err
}

func (e *UserAdapter) Patch(ctx context.Context, user map[string]interface{}) (int64, error) {
	return es.Patch(ctx, e.Client, "users", user, e.idJson)
}

func (e *UserAdapter) Delete(ctx context.Context, id string) (int64, error) {
	return es.Delete(ctx, e.Client, "users", id)
}
