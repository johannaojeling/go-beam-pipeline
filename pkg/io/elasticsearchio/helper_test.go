package elasticsearchio

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/elastic/go-elasticsearch/v8/esutil"
)

type SearchBody struct {
	Query json.RawMessage `json:"query"`
}

func NewClient(t *testing.T, addresses []string) *elasticsearch.Client {
	t.Helper()

	config := elasticsearch.Config{
		Addresses: addresses,
	}

	client, err := elasticsearch.NewClient(config)
	if err != nil {
		t.Fatalf("error creating client: %v", err)
	}

	return client
}

func RefreshIndices(
	ctx context.Context,
	t *testing.T,
	client *elasticsearch.Client,
	indices []string,
) {
	t.Helper()

	refreshRequest := esapi.IndicesRefreshRequest{
		Index: indices,
	}

	response, err := refreshRequest.Do(ctx, client)
	if err != nil {
		t.Fatalf("error calling Refresh API: %v", err)
	}

	defer response.Body.Close()

	if response.IsError() {
		t.Fatalf("error in response: %v", err)
	}
}

func DeleteIndices(
	ctx context.Context,
	t *testing.T,
	client *elasticsearch.Client,
	indices []string,
) {
	t.Helper()

	response, err := client.Indices.Delete(
		indices,
		client.Indices.Delete.WithContext(ctx),
		client.Indices.Delete.WithIgnoreUnavailable(true),
	)
	if err != nil {
		t.Fatalf("error calling Delete API: %v", err)
	}

	defer response.Body.Close()

	if response.IsError() {
		t.Fatalf("error in response: %v", response.String())
	}
}

func IndexDocuments(
	ctx context.Context,
	t *testing.T,
	client *elasticsearch.Client,
	index string,
	documents []map[string]any,
) {
	t.Helper()

	config := esutil.BulkIndexerConfig{
		Client: client,
		Index:  index,
	}

	bulkIndexer, err := esutil.NewBulkIndexer(config)
	if err != nil {
		t.Fatalf("error initializing bulk indexer: %v", err)
	}

	for _, doc := range documents {
		if err := addDocument(ctx, bulkIndexer, doc); err != nil {
			t.Fatalf("error adding document: %v", err)
		}
	}

	if err := bulkIndexer.Close(ctx); err != nil {
		t.Fatalf("error closing bulk indexer: %v", err)
	}

	stats := bulkIndexer.Stats()
	if numFailed := stats.NumFailed; numFailed > 0 {
		t.Fatalf("error indexing %d document(s)", numFailed)
	}
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
	t *testing.T,
	client *elasticsearch.Client,
	index string,
	query string,
) []map[string]any {
	t.Helper()

	searchBody := &SearchBody{
		Query: []byte(query),
	}
	from := 0
	size := 10

	var records []map[string]any
	if err := search(ctx, client, index, searchBody, from, size, true, &records); err != nil {
		t.Fatalf("error searching index: %v", err)
	}

	return records
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
