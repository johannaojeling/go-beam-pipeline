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

func NewClient(ctx context.Context, addresses []string) (*elasticsearch.Client, error) {
	config := elasticsearch.Config{
		Addresses: addresses,
	}

	client, err := elasticsearch.NewClient(config)
	if err != nil {
		return nil, fmt.Errorf("error creating client: %w", err)
	}

	return client, nil
}

func RefreshIndices(ctx context.Context, client *elasticsearch.Client, indices []string) error {
	refreshRequest := esapi.IndicesRefreshRequest{
		Index: indices,
	}

	response, err := refreshRequest.Do(ctx, client)
	if err != nil {
		return fmt.Errorf("error calling Refresh API: %w", err)
	}

	defer response.Body.Close()

	if response.IsError() {
		return fmt.Errorf("error in response: %w", err)
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
		return fmt.Errorf("error calling Delete API: %w", err)
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
		return fmt.Errorf("error initializing bulk indexer: %w", err)
	}

	for _, doc := range documents {
		err := addDocument(ctx, bulkIndexer, doc)
		if err != nil {
			return fmt.Errorf("error adding document to bulk indexer: %w", err)
		}
	}

	if err := bulkIndexer.Close(ctx); err != nil {
		return fmt.Errorf("error closing bulk indexer: %w", err)
	}

	stats := bulkIndexer.Stats()
	if numFailed := stats.NumFailed; numFailed > 0 {
		return fmt.Errorf("error indexing %d document(s)", numFailed)
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
		return fmt.Errorf("error encoding document: %w", err)
	}

	body := bytes.NewReader(data)
	item := esutil.BulkIndexerItem{
		Action: "index",
		Body:   body,
	}

	if err := bulkIndexer.Add(ctx, item); err != nil {
		return fmt.Errorf("error adding item: %w", err)
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
	if err := search(ctx, client, index, searchBody, from, size, true, &records); err != nil {
		return nil, fmt.Errorf("error searching index: %w", err)
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
		return fmt.Errorf("error calling Search API: %w", err)
	}

	defer response.Body.Close()

	if response.IsError() {
		return fmt.Errorf("error in response: %v", response.String())
	}

	searchResponse := &SearchResponse{}
	if err := json.NewDecoder(response.Body).Decode(searchResponse); err != nil {
		return fmt.Errorf("error parsing response body: %w", err)
	}

	hits := searchResponse.Hits.Hits

	if len(*records) == 0 {
		total := searchResponse.Hits.Total.Value
		*records = make([]map[string]any, 0, total)
	}

	for _, hit := range hits {
		var record map[string]any
		if err := json.Unmarshal(hit.Source, &record); err != nil {
			return fmt.Errorf("error unmarshaling source: %w", err)
		}

		*records = append(*records, record)
	}

	if len(hits) == size {
		if err := search(ctx, client, index, searchBody, from+size, size, false, records); err != nil {
			return fmt.Errorf("error searching: %w", err)
		}
	}

	return nil
}
