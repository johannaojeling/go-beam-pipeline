package es

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/elastic/go-elasticsearch/v8/esutil"
)

type SearchBody struct {
	Query json.RawMessage `json:"query"`
}

type SearchResponse struct {
	Hits struct {
		Hits []struct {
			Source json.RawMessage `json:"_source"`
		} `json:"hits"`
		Total struct {
			Value int `json:"value"`
		} `json:"total"`
	} `json:"hits"`
}

func RefreshIndices(ctx context.Context, client *elasticsearch.Client, indices []string) error {
	refreshRequest := esapi.IndicesRefreshRequest{
		Index: indices,
	}
	response, err := refreshRequest.Do(ctx, client)
	if err != nil {
		return fmt.Errorf("failed to call Refresh API: %v", err)
	}

	defer response.Body.Close()
	if response.IsError() {
		return fmt.Errorf("error in response: %v", err)
	}
	return nil
}

func DeleteIndices(
	ctx context.Context,
	client *elasticsearch.Client,
	indices []string,
) error {
	response, err := client.Indices.Delete(
		indices,
		client.Indices.Delete.WithContext(ctx),
		client.Indices.Delete.WithIgnoreUnavailable(true),
	)
	if err != nil {
		return fmt.Errorf("failed to call Delete API: %v", err)
	}

	defer response.Body.Close()
	if response.IsError() {
		return fmt.Errorf("error in response: %v", response.String())
	}
	return nil
}

func ReadDocuments(
	ctx context.Context,
	client *elasticsearch.Client,
	index string,
	query string,
) ([]map[string]interface{}, error) {
	searchBody := &SearchBody{
		Query: []byte(query),
	}
	from := 0
	size := 10
	var records []map[string]interface{}

	err := search(ctx, client, index, searchBody, from, size, true, &records)
	if err != nil {
		return nil, fmt.Errorf("failed to search index: %v", err)
	}

	return records, nil
}

func search(
	ctx context.Context,
	client *elasticsearch.Client,
	index string,
	searchBody *SearchBody,
	from int,
	size int,
	trackTotal bool,
	records *[]map[string]interface{},
) error {
	body := esutil.NewJSONReader(searchBody)
	response, err := client.Search(
		client.Search.WithContext(ctx),
		client.Search.WithIndex(index),
		client.Search.WithFrom(from),
		client.Search.WithBody(body),
		client.Search.WithSize(size),
		client.Search.WithTrackTotalHits(trackTotal),
	)
	if err != nil {
		return fmt.Errorf("failed to call Search API: %v", err)
	}

	defer response.Body.Close()
	if response.IsError() {
		return fmt.Errorf("error in response: %v", response.String())
	}

	searchResponse := new(SearchResponse)
	err = json.NewDecoder(response.Body).Decode(searchResponse)
	if err != nil {
		return fmt.Errorf("failed to parse response body: %v", err)
	}

	hits := searchResponse.Hits.Hits

	if len(*records) == 0 {
		total := searchResponse.Hits.Total.Value
		*records = make([]map[string]interface{}, 0, total)
	}

	for _, hit := range hits {
		var record map[string]interface{}
		err := json.Unmarshal(hit.Source, &record)
		if err != nil {
			return fmt.Errorf("failed to unmarshal source: %v", err)
		}
		*records = append(*records, record)
	}

	if len(hits) == size {
		err := search(ctx, client, index, searchBody, from+size, size, false, records)
		if err != nil {
			return fmt.Errorf("failed to search: %v", err)
		}
	}
	return nil
}
