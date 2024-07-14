package adapter

import (
	"context"
	"fmt"
	"reflect"

	es "github.com/core-go/elasticsearch"
	"github.com/elastic/go-elasticsearch/v8"

	"go-service/internal/user/model"
)

type UserAdapter struct {
	Client  *elasticsearch.Client
	Index   string
	idIndex int
	idJson  string
	Map     []es.FieldMap
}

func NewUserRepository(client *elasticsearch.Client) *UserAdapter {
	userType := reflect.TypeOf(model.User{})
	idIndex, _, idJson := es.FindIdField(userType)
	return &UserAdapter{Client: client, Index: "users", idIndex: idIndex, idJson: idJson, Map: es.BuildMap(userType)}
}

func (a *UserAdapter) All(ctx context.Context) ([]model.User, error) {
	var users []model.User
	query := make(map[string]interface{})
	err := es.Find(ctx, a.Client, []string{"users"}, query, &users, a.idJson)
	return users, err
}

func (a *UserAdapter) Load(ctx context.Context, id string) (*model.User, error) {
	var user model.User
	ok, err := es.FindOne(ctx, a.Client, a.Index, id, &user, a.idJson)
	if !ok || err != nil {
		return nil, err
	}
	return &user, nil
}

func (a *UserAdapter) Create(ctx context.Context, user *model.User) (int64, error) {
	return es.Create(ctx, a.Client, a.Index, es.BuildBody(user, a.Map), user.Id)
}

func (a *UserAdapter) Update(ctx context.Context, user *model.User) (int64, error) {
	if len(user.Id) == 0 {
		return -1, fmt.Errorf("require Id Field '%s' of User struct for update", "Id")
	}
	res, err := es.Update(ctx, a.Client, a.Index, es.BuildBody(user, a.Map), user.Id)
	return res, err
}
func (a *UserAdapter) Save(ctx context.Context, user *model.User) (int64, error) {
	res, err := es.Save(ctx, a.Client, a.Index, es.BuildBody(user, a.Map), user.Id)
	return res, err
}

func (a *UserAdapter) Patch(ctx context.Context, user map[string]interface{}) (int64, error) {
	return es.Patch(ctx, a.Client, a.Index, user, a.idJson)
}

func (a *UserAdapter) Delete(ctx context.Context, id string) (int64, error) {
	return es.Delete(ctx, a.Client, a.Index, id)
}
