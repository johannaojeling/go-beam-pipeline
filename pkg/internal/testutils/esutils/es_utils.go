package esutils

import (
	"bytes"
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

func IndexDocuments(
	ctx context.Context,
	client *elasticsearch.Client,
	index string,
	documents []map[string]any,
) error {
	config := esutil.BulkIndexerConfig{
		Client: client,
		Index:  index,
	}
	bulkIndexer, err := esutil.NewBulkIndexer(config)
	if err != nil {
		return fmt.Errorf("failed to initialize bulk indexer: %v", err)
	}

	for _, doc := range documents {
		err := addDocument(ctx, bulkIndexer, doc)
		if err != nil {
			return fmt.Errorf("failed to add document to bulk indexer: %v", err)
		}
	}
	err = bulkIndexer.Close(ctx)
	if err != nil {
		return fmt.Errorf("failed to close bulk indexer: %v", err)
	}

	stats := bulkIndexer.Stats()
	if numFailed := stats.NumFailed; numFailed > 0 {
		return fmt.Errorf("failed to index %d document", numFailed)
	}

	return nil
}

func addDocument(
	ctx context.Context,
	bulkIndexer esutil.BulkIndexer,
	document map[string]any,
) error {
	data, err := json.Marshal(document)
	if err != nil {
		return fmt.Errorf("failed to encode document: %v", err)
	}
	body := bytes.NewReader(data)

	err = bulkIndexer.Add(
		ctx,
		esutil.BulkIndexerItem{
			Action: "index",
			Body:   body,
		},
	)
	if err != nil {
		return fmt.Errorf("failed to add item: %v", err)
	}
	return nil
}

func SearchDocuments(
	ctx context.Context,
	client *elasticsearch.Client,
	index string,
	query string,
) ([]map[string]any, error) {
	searchBody := &SearchBody{
		Query: []byte(query),
	}
	from := 0
	size := 10
	var records []map[string]any

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
	records *[]map[string]any,
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
		*records = make([]map[string]any, 0, total)
	}

	for _, hit := range hits {
		var record map[string]any
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
