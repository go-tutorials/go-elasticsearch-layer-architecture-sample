package elasticsearch

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/elastic/go-elasticsearch/v8/esutil"
	"reflect"
	"strings"
)

func BuildSearchResult(ctx context.Context, db *elasticsearch.Client, results interface{}, indexName string, query map[string]interface{}, sort []map[string]interface{}, limit int64, offset int64, modelType reflect.Type) (int64, error) {
	from := int(offset)
	size := int(limit)
	fullQuery := UpdateQuery(query)
	fullQuery["sort"] = sort
	req := esapi.SearchRequest{
		Index: []string{indexName},
		Body:  esutil.NewJSONReader(fullQuery),
		From:  &from,
		Size:  &size,
	}

	res, err := req.Do(ctx, db)
	if err != nil {
		return 0, err
	}
	defer res.Body.Close()
	var count int64
	if res.IsError() {
		return 0, errors.New("response error")
	} else {
		var r map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
			return 0, err
		} else {
			hits := r["hits"].(map[string]interface{})["hits"].([]interface{})
			count = int64(r["hits"].(map[string]interface{})["total"].(map[string]interface{})["value"].(float64))
			listResults := make([]interface{}, 0)
			idField := modelType.Field(0)
			jsonID := idField.Tag.Get("json")
			for _, hit := range hits {
				r := hit.(map[string]interface{})["_source"]
				r.(map[string]interface{})[jsonID] = hit.(map[string]interface{})["_id"]
				stValue := reflect.New(modelType).Elem()
				for i := 0; i < modelType.NumField(); i++ {
					field := modelType.Field(i)
					if value, ok := r.(map[string]interface{})[field.Name]; ok {
						stValue.Field(i).Set(reflect.ValueOf(value))
					}
				}
				listResults = append(listResults, r)
			}

			err := json.NewDecoder(esutil.NewJSONReader(listResults)).Decode(results)
			if err != nil {
				return count, err
			}
			return count, err
		}
	}
}

func BuildSort(s string, modelType reflect.Type) []map[string]interface{} {
	sort := []map[string]interface{}{}
	if len(s) == 0 {
		return sort
	}
	sorts := strings.Split(s, ",")
	for i := 0; i < len(sorts); i++ {
		sortField := strings.TrimSpace(sorts[i])
		fieldName := sortField

		var mapFieldName map[string]interface{}
		c := sortField[0:1]
		if c == "-" || c == "+" {
			//fieldName = sortField[1:]
			field, ok := getFieldName(modelType, sortField[1:])
			if !ok {
				return []map[string]interface{}{}
			}
			fieldName = field
			if c == "-" {
				mapFieldName = map[string]interface{}{
					fieldName: map[string]string{
						"order": "desc",
					},
				}
			} else {
				mapFieldName = map[string]interface{}{
					fieldName: map[string]string{
						"order": "asc",
					},
				}
			}
		}
		sort = append(sort, mapFieldName)
	}

	return sort
}

func getFieldName(structType reflect.Type, jsonTagValue string) (string, bool) {
	var (
		bsonTagValue string
		typeField    reflect.Kind
	)
	for i := 0; i < structType.NumField(); i++ {
		field := structType.Field(i)
		jsonTag := field.Tag.Get("json")
		if jsonTag == jsonTagValue {
			bsonTagValue = field.Tag.Get("bson")
			typeField = field.Type.Kind()
			break
		}
	}
	if bsonTagValue != "_id" {
		if typeField == reflect.String {
			return "", false
		}
		return jsonTagValue, true
	}
	return bsonTagValue, true
}
