package repository

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/elastic/go-elasticsearch/v8/esutil"
	"reflect"
	"strings"

	"go-service/internal/model"
)

type UserAdapter struct {
	client *elasticsearch.Client
}

func NewUserRepository(client *elasticsearch.Client) *UserAdapter {
	return &UserAdapter{client: client}
}

func convertDocToJson(doc interface{}) (string, error) {
	jsonString, err := json.Marshal(doc)

	if err != nil {
		return "", err
	}
	return string(jsonString), err
}

func (e *UserAdapter) All(ctx context.Context) ([]model.User, error) {
	var users []model.User
	var mapResponse map[string]interface{}
	var buf bytes.Buffer

	query := `{
  "query": {
    "match_all": {}
  }
}`

	var queryString = strings.NewReader(query)

	err := json.NewEncoder(&buf).Encode(&queryString)
	if err != nil {
		return users, err
	}

	result, err := e.client.Search(
		e.client.Search.WithContext(ctx),
		e.client.Search.WithIndex("users"),
		e.client.Search.WithBody(queryString),
		e.client.Search.WithTrackTotalHits(true),
		e.client.Search.WithPretty(),
	)
	defer result.Body.Close()

	err = json.NewDecoder(result.Body).Decode(&mapResponse)
	if err != nil {
		return users, err
	}

	var u = &model.User{}
	for _, hit := range mapResponse["hits"].(map[string]interface{})["hits"].([]interface{}) {
		user := hit.(map[string]interface{})

		source := user["_source"]
		u.Id = user["_id"].(string)

		bytes, err := json.Marshal(source)
		if err != nil {
			return users, err
		}
		err = json.Unmarshal(bytes, u)
		if err != nil {
			return users, err
		}
		users = append(users, *u)
	}
	return users, nil
}

func (e *UserAdapter) Load(ctx context.Context, id string) (*model.User, error) {
	var mapResponse map[string]interface{}
	var buf bytes.Buffer

	query := `{
  "query": {
    "match": { "id": "{0}" }
  },
  "size": 1
}`

	query = strings.Replace(query, "{0}", id, 1)
	var queryString = strings.NewReader(query)

	err := json.NewEncoder(&buf).Encode(&queryString)
	if err != nil {
		return nil, err
	}

	result, err := e.client.Search(
		e.client.Search.WithContext(ctx),
		e.client.Search.WithIndex("users"),
		e.client.Search.WithBody(queryString),
		e.client.Search.WithTrackTotalHits(true),
		e.client.Search.WithPretty(),
	)
	defer result.Body.Close()

	err = json.NewDecoder(result.Body).Decode(&mapResponse)
	if err != nil {
		return nil, err
	}

	var u = &model.User{}
	for _, hit := range mapResponse["hits"].(map[string]interface{})["hits"].([]interface{}) {
		user := hit.(map[string]interface{})

		source := user["_source"]
		bytes, err:= json.Marshal(source)
		if err != nil {
			return nil, err
		}
		err = json.Unmarshal(bytes, u)
		if err != nil {
			return nil, err
		}
		return u, nil
	}
	return nil, nil
}

func (e *UserAdapter) Create(ctx context.Context, user *model.User) (int64, error) {
	if user == nil {
		return 0, nil
	}

	userJsonString, err := convertDocToJson(user)
	if err != nil {
		return 0, err
	}
	request := esapi.IndexRequest{
		Index:      "users",
		DocumentID: user.Id,
		Body:       strings.NewReader(userJsonString),
		Refresh:    "true",
	}
	response, err := request.Do(ctx, e.client)

	if err != nil {
		return 0, err
	}

	defer response.Body.Close()

	var result map[string]interface{}

	err = json.NewDecoder(response.Body).Decode(&result)
	if err != nil {
		return -1, err
	}
	return 1, nil
}

func (e *UserAdapter) Update(ctx context.Context, user *model.User) (int64, error) {
	query := map[string]interface{}{
		"doc": user,
	}
	request := esapi.UpdateRequest{
		Index:      "users",
		DocumentID: user.Id,
		Body:       esutil.NewJSONReader(query),
		Refresh:    "true",
	}
	response, err := request.Do(ctx, e.client)

	if err != nil {
		return -1, err
	}

	defer response.Body.Close()

	var result map[string]interface{}

	err = json.NewDecoder(response.Body).Decode(&result)

	if err != nil {
		return -1, err
	}
	return 1, nil
}

func (e *UserAdapter) Patch(ctx context.Context, user map[string]interface{}) (int64, error) {
	var userId = reflect.ValueOf(user["id"])
	delete(user, "id")
	request := esapi.UpdateRequest{
		Index:      "users",
		DocumentID: userId.String(),
		Body:       esutil.NewJSONReader(map[string]interface{}{"doc": user}),
		Refresh:    "true",
	}
	response, err := request.Do(ctx, e.client)

	if err != nil {
		return -1, err
	}

	defer response.Body.Close()

	var result map[string]interface{}
	err = json.NewDecoder(response.Body).Decode(&result)
	if err != nil {
		return -1, err
	}
	return 1, nil
}

func (e *UserAdapter) Delete(ctx context.Context, id string) (int64, error) {
	request := esapi.DeleteRequest{
		Index:      "users",
		DocumentID: id,
	}
	response, err := request.Do(ctx, e.client)

	if err != nil {
		return -1, err
	}

	defer response.Body.Close()

	var result map[string]interface{}
	err = json.NewDecoder(response.Body).Decode(&result)
	if err != nil {
		return -1, err
	}
	return 1, nil
}
