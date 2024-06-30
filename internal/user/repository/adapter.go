package repository

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	es "github.com/core-go/elasticsearch"
	"github.com/elastic/go-elasticsearch/v8"

	"go-service/internal/user/model"
)

type UserAdapter struct {
	Client  *elasticsearch.Client
	Index   string
	idIndex int
	idJson  string
	Map     []FieldMap
}
type FieldMap struct {
	Index int
	Json  string
	Id    bool
}

func BuildMap(modelType reflect.Type) []FieldMap {
	var fms []FieldMap
	numField := modelType.NumField()
	for i := 0; i < numField; i++ {
		field := modelType.Field(i)
		bsonTag := field.Tag.Get("bson")
		tags := strings.Split(bsonTag, ",")
		json := field.Name
		if tag1, ok1 := field.Tag.Lookup("json"); ok1 {
			json = strings.Split(tag1, ",")[0]
		}
		fm := FieldMap{Index: i, Json: json}
		for _, tag := range tags {
			if strings.TrimSpace(tag) == "_id" {
				fm.Id = true
			}
		}
		fms = append(fms, fm)
	}
	return fms
}
func BuildDocument(model interface{}, fields []FieldMap) map[string]interface{} {
	vo := reflect.ValueOf(model)
	if vo.Kind() == reflect.Ptr {
		vo = reflect.Indirect(vo)
	}
	result := map[string]interface{}{}
	le := len(fields)
	for i := 0; i < le; i++ {
		if !fields[i].Id {
			result[fields[i].Json] = vo.Field(fields[i].Index).Interface()
		}
	}
	return result
}
func NewUserRepository(client *elasticsearch.Client) *UserAdapter {
	userType := reflect.TypeOf(model.User{})
	idIndex, _, idJson := es.FindIdField(userType)
	return &UserAdapter{Client: client, Index: "users", idIndex: idIndex, idJson: idJson, Map: BuildMap(userType)}
}

func (a *UserAdapter) All(ctx context.Context) ([]model.User, error) {
	var users []model.User
	query := make(map[string]interface{})
	err := es.Find(ctx, a.Client, []string{"users"}, query, &users)
	return users, err
}

func (a *UserAdapter) Load(ctx context.Context, id string) (*model.User, error) {
	var user model.User
	ok, err := es.FindOne(ctx, a.Client, a.Index, id, &user)
	if !ok || err != nil {
		return nil, err
	}
	return &user, nil
}

func (a *UserAdapter) Create(ctx context.Context, user *model.User) (int64, error) {
	var u model.User
	u = *user
	return es.Create(ctx, a.Client, a.Index, BuildDocument(u, a.Map), &user.Id)
}

func (a *UserAdapter) Update(ctx context.Context, user *model.User) (int64, error) {
	if len(user.Id) == 0 {
		return -1, fmt.Errorf("require Id Field '%s' of User struct for update", "Id")
	}
	res, err := es.Update(ctx, a.Client, a.Index, BuildDocument(user, a.Map), user.Id)
	return res, err
}
func (a *UserAdapter) Save(ctx context.Context, user *model.User) (int64, error) {
	res, err := es.Save(ctx, a.Client, a.Index, BuildDocument(user, a.Map), user.Id)
	return res, err
}

func (a *UserAdapter) Patch(ctx context.Context, user map[string]interface{}) (int64, error) {
	return es.Patch(ctx, a.Client, a.Index, user, a.idJson)
}

func (a *UserAdapter) Delete(ctx context.Context, id string) (int64, error) {
	return es.Delete(ctx, a.Client, a.Index, id)
}
