package repository

import (
	"context"
	"reflect"

	"github.com/elastic/go-elasticsearch/v8"

	"go-service/internal/user/model"
	es "go-service/pkg/elasticsearch"
)

type UserAdapter struct {
	client     *elasticsearch.Client
	idIndex    int
	jsonIdName string
	Map        map[string]string
}

func NewUserRepository(client *elasticsearch.Client) *UserAdapter {
	userType := reflect.TypeOf(model.User{})
	idIndex, _, jsonIdName := es.FindIdField(userType)
	mp := es.MakeMapJson(userType)
	return &UserAdapter{client: client, idIndex: idIndex, jsonIdName: jsonIdName, Map: mp}
}

func (e *UserAdapter) All(ctx context.Context) ([]model.User, error) {
	var users []model.User
	query := make(map[string]interface{})
	if ok, err := es.Find(ctx, e.client, []string{"users"}, query, &users); ok {
		return users, nil
	} else {
		return nil, err
	}
}
func (e *UserAdapter) Load(ctx context.Context, id string) (*model.User, error) {
	var user model.User
	ok, err := es.FindOne(ctx, e.client, "users", id, &user)
	if !ok || err != nil {
		return nil, err
	}
	return &user, nil
}

func (e *UserAdapter) Create(ctx context.Context, user *model.User) (int64, error) {
	return es.Create(ctx, e.client, "users", e.idIndex)
}

func (e *UserAdapter) Update(ctx context.Context, user *model.User) (int64, error) {
	return es.Update(ctx, e.client, "users", user, e.idIndex)
}
func (e *UserAdapter) Patch(ctx context.Context, user map[string]interface{}) (int64, error) {
	return es.Patch(ctx, e.client, "users", e.jsonIdName, es.MapToDBObject(user, e.Map))
}

func (e *UserAdapter) Delete(ctx context.Context, id string) (int64, error) {
	return es.Delete(ctx, e.client, "users", id)
}
