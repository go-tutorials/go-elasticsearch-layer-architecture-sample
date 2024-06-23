package elasticsearch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/elastic/go-elasticsearch/v8/esutil"
	"log"
	"reflect"
	"strings"
)

func FindIdField(modelType reflect.Type) (int, string, string) {
	return findBsonField(modelType, "_id")
}
func findBsonField(modelType reflect.Type, bsonName string) (int, string, string) {
	numField := modelType.NumField()
	for i := 0; i < numField; i++ {
		field := modelType.Field(i)
		bsonTag := field.Tag.Get("bson")
		tags := strings.Split(bsonTag, ",")
		json := field.Name
		if tag1, ok1 := field.Tag.Lookup("json"); ok1 {
			json = strings.Split(tag1, ",")[0]
		}
		for _, tag := range tags {
			if strings.TrimSpace(tag) == bsonName {
				return i, field.Name, json
			}
		}
	}
	return -1, "", ""
}
func FindFieldByName(modelType reflect.Type, fieldName string) (index int, jsonTagName string) {
	numField := modelType.NumField()
	for index := 0; index < numField; index++ {
		field := modelType.Field(index)
		if field.Name == fieldName {
			jsonTagName := fieldName
			if jsonTag, ok := field.Tag.Lookup("json"); ok {
				jsonTagName = strings.Split(jsonTag, ",")[0]
			}
			return index, jsonTagName
		}
	}
	return -1, fieldName
}

func FindFieldByJson(modelType reflect.Type, jsonTagName string) (index int, fieldName string) {
	numField := modelType.NumField()
	for i := 0; i < numField; i++ {
		field := modelType.Field(i)
		tag1, ok1 := field.Tag.Lookup("json")
		if ok1 && strings.Split(tag1, ",")[0] == jsonTagName {
			return i, field.Name
		}
	}
	return -1, jsonTagName
}

func FindFieldByIndex(modelType reflect.Type, fieldIndex int) (fieldName, jsonTagName string) {
	if fieldIndex < modelType.NumField() {
		field := modelType.Field(fieldIndex)
		jsonTagName := ""
		if jsonTag, ok := field.Tag.Lookup("json"); ok {
			jsonTagName = strings.Split(jsonTag, ",")[0]
		}
		return field.Name, jsonTagName
	}
	return "", ""
}

func BuildQueryWithoutIdFromObject(object interface{}) map[string]interface{} {
	valueOf := reflect.Indirect(reflect.ValueOf(object))
	idIndex, _, _ := FindIdField(valueOf.Type())
	result := map[string]interface{}{}
	for i := 0; i < valueOf.NumField(); i++ {
		if i != idIndex {
			_, jsonName := FindFieldByIndex(valueOf.Type(), i)
			result[jsonName] = valueOf.Field(i).Interface()
		}
	}
	return result
}

func Exist(ctx context.Context, es *elasticsearch.Client, indexName string, documentID string) (bool, error) {
	req := esapi.ExistsRequest{
		Index:      indexName,
		DocumentID: documentID,
	}
	res, err := req.Do(ctx, es)
	if err != nil {
		return false, err
	}
	defer res.Body.Close()

	if res.IsError() {
		return false, errors.New("response error")
	} else {
		var r map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
			return false, err
		} else {
			return r["found"].(bool), nil
		}
	}
}

func FindOne(ctx context.Context, es *elasticsearch.Client, indexName string, documentID string, result interface{}) (bool, error) {
	req := esapi.GetRequest{
		Index:      indexName,
		DocumentID: documentID,
	}
	res, err := req.Do(ctx, es)
	if err != nil {
		return false, err
	}
	defer res.Body.Close()

	if !res.IsError() {
		var r map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&r); err == nil {
			hit := r["_source"].(map[string]interface{})
			hit["id"] = r["_id"]
			if err := json.NewDecoder(esutil.NewJSONReader(hit)).Decode(&result); err != nil {
				return false, err
			}
			return true, nil
		}
		return false, err
	}
	return false, errors.New("response error")
}

func FindOneByFilter(ctx context.Context, es *elasticsearch.Client, index []string, filter map[string]interface{}, result interface{}) (bool, error) {
	req := esapi.SearchRequest{
		Index:          index,
		Body:           esutil.NewJSONReader(filter),
		TrackTotalHits: true,
		Pretty:         true,
	}
	res, err := req.Do(ctx, es)
	if err != nil {
		return false, err
	}
	defer res.Body.Close()

	if res.IsError() {
		return false, errors.New("response error")
	} else {
		var r map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
			return false, err
		} else {
			hits := r["hits"].(map[string]interface{})["hits"].([]interface{})
			total := int(r["hits"].(map[string]interface{})["total"].(map[string]interface{})["value"].(float64))
			if total >= 1 {
				if err := json.NewDecoder(esutil.NewJSONReader(hits[0])).Decode(&result); err != nil {
					return false, err
				}
				return true, nil
			}
			return false, nil
		}
	}
}

func Find(ctx context.Context, es *elasticsearch.Client, indexName []string, query map[string]interface{}, result interface{}) error {
	req := esapi.SearchRequest{
		Index:          indexName,
		Body:           esutil.NewJSONReader(query),
		TrackTotalHits: true,
		Pretty:         true,
	}
	res, err := req.Do(ctx, es)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	modelType := reflect.TypeOf(result).Elem().Elem()
	if res.IsError() {
		return errors.New("response error")
	} else {
		var r map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
			return err
		} else {
			hits := r["hits"].(map[string]interface{})["hits"].([]interface{})
			listResults := make([]interface{}, 0)
			for _, hit := range hits {
				r := hit.(map[string]interface{})["_source"]
				r.(map[string]interface{})["id"] = hit.(map[string]interface{})["_id"]
				stValue := reflect.New(modelType).Elem()
				for i := 0; i < modelType.NumField(); i++ {
					field := modelType.Field(i)
					if value, ok := r.(map[string]interface{})[field.Name]; ok {
						stValue.Field(i).Set(reflect.ValueOf(value))
					}
				}
				listResults = append(listResults, r)
			}
			return json.NewDecoder(esutil.NewJSONReader(listResults)).Decode(result)
		}
	}
}

func Create(ctx context.Context, es *elasticsearch.Client, indexName string, model interface{}, id *string) (int64, error) {
	var req esapi.CreateRequest
	if id != nil {
		body := BuildQueryWithoutIdFromObject(model)
		req = esapi.CreateRequest{
			Index:      indexName,
			DocumentID: *id,
			Body:       esutil.NewJSONReader(body),
			Refresh:    "true",
		}
	} else {
		req = esapi.CreateRequest{
			Index:   indexName,
			Body:    esutil.NewJSONReader(model),
			Refresh: "true",
		}
	}
	res, err := req.Do(ctx, es)
	if err != nil {
		return -1, err
	}
	defer res.Body.Close()
	if res.IsError() {
		return 0, nil
	} else {
		var r map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
			return -1, err
		} else {
			log.Printf("[%s] %s; version=%d", res.Status(), r["result"], int(r["_version"].(float64)))
			return int64(r["_version"].(float64)), nil
		}
	}
}

func Update(ctx context.Context, es *elasticsearch.Client, indexName string, model interface{}, id string) (int64, error) {
	query := map[string]interface{}{
		"doc": model,
	}
	req := esapi.UpdateRequest{
		Index:      indexName,
		DocumentID: id,
		Body:       esutil.NewJSONReader(query),
		Refresh:    "true",
	}
	res, err := req.Do(ctx, es)
	if err != nil {
		return -1, err
	}
	defer res.Body.Close()
	if res.IsError() {
		return -1, errors.New("document ID not exists in the index")
	} else {
		var r map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
			return -1, err
		} else {
			successful := int64(r["_shards"].(map[string]interface{})["successful"].(float64))
			return successful, nil
		}
	}
}

func Save(ctx context.Context, es *elasticsearch.Client, indexName string, model interface{}, id string) (int64, error) {
	// body := BuildQueryWithoutIdFromObject(model)
	query := map[string]interface{}{
		"doc": model,
	}
	req := esapi.IndexRequest{
		Index:      indexName,
		DocumentID: id,
		Body:       esutil.NewJSONReader(query),
		Refresh:    "true",
	}
	res, err := req.Do(ctx, es)
	if err != nil {
		return -1, err
	}
	defer res.Body.Close()
	if res.IsError() {
		return -1, errors.New("document ID not exists in the index")
	}
	var r map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		return -1, err
	}
	successful := int64(r["_shards"].(map[string]interface{})["successful"].(float64))
	return successful, nil
}

func Patch(ctx context.Context, es *elasticsearch.Client, indexName string, model map[string]interface{}, idName string) (int64, error) {
	idValue, ok := model[idName]
	if !ok {
		return -1, fmt.Errorf("%s must be in map[string]interface{} for patch", idName)
	}
	id, ok2 := idValue.(string)
	if !ok2 {
		return -1, fmt.Errorf("%s map[string]interface{} must be a string for patch", idName)
	}
	delete(model, idName)
	query := map[string]interface{}{
		"doc": model,
	}
	req := esapi.UpdateRequest{
		Index:      indexName,
		DocumentID: id,
		Body:       esutil.NewJSONReader(query),
		Refresh:    "true",
	}
	res, err := req.Do(ctx, es)
	model[idName] = id
	if err != nil {
		return -1, err
	}
	defer res.Body.Close()
	if res.IsError() {
		return -1, errors.New("document ID not exists in the index")
	} else {
		var r map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
			return -1, err
		} else {
			successful := int64(r["_shards"].(map[string]interface{})["successful"].(float64))
			return successful, nil
		}
	}
}

func Delete(ctx context.Context, es *elasticsearch.Client, indexName string, documentID string) (int64, error) {
	req := esapi.DeleteRequest{
		Index:      indexName,
		DocumentID: documentID,
	}
	res, err := req.Do(ctx, es)
	if err != nil {
		return -1, err
	}
	defer res.Body.Close()
	if res.IsError() {
		return -1, errors.New("document ID not exists in the index")
	} else {
		var r map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
			return -1, err
		} else {
			successful := int64(r["_shards"].(map[string]interface{})["successful"].(float64))
			return successful, nil
		}
	}
}
