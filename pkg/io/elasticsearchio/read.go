package elasticsearchio

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
	"github.com/elastic/go-elasticsearch/v8/esutil"
)

const (
	defaultReadBatchSize = 100
	defaultReadKeepAlive = "5m"
)

func init() {
	register.DoFn3x1[context.Context, []byte, func(beam.X), error](&readFn{})
	register.Emitter1[beam.X]()
}

type ReadConfig struct {
	Addresses []string
	CloudId   string
	ApiKey    string
	Index     string
	Query     string
	BatchSize int
	KeepAlive string
}

func Read(
	scope beam.Scope,
	cfg ReadConfig,
	elemType reflect.Type,
) beam.PCollection {
	scope = scope.Scope("elasticsearchio.Read")
	impulse := beam.Impulse(scope)

	return beam.ParDo(
		scope,
		newReadFn(cfg, elemType),
		impulse,
		beam.TypeDefinition{Var: beam.XType, T: elemType},
	)
}

type readFn struct {
	esFn
	Query     string
	BatchSize int
	KeepAlive string
}

func newReadFn(
	cfg ReadConfig,
	elemType reflect.Type,
) *readFn {
	batchSize := cfg.BatchSize
	if batchSize <= 0 {
		batchSize = defaultReadBatchSize
	}
	keepAlive := cfg.KeepAlive
	if keepAlive == "" {
		keepAlive = defaultReadKeepAlive
	}

	return &readFn{
		esFn: esFn{
			Addresses: cfg.Addresses,
			CloudId:   cfg.CloudId,
			ApiKey:    cfg.ApiKey,
			Index:     cfg.Index,
			Type:      beam.EncodedType{T: elemType},
		},
		Query:     cfg.Query,
		BatchSize: batchSize,
		KeepAlive: keepAlive,
	}
}

func (fn *readFn) ProcessElement(
	ctx context.Context,
	_ []byte,
	emit func(beam.X),
) error {
	pitResponse, err := fn.openPIT(ctx)
	if err != nil {
		return fmt.Errorf("error opening Point In Time: %v", err)
	}

	pit := &PointInTime{
		Id:        pitResponse.Id,
		KeepAlive: fn.KeepAlive,
	}
	query := []byte(fn.Query)

	searchRequest := &SearchRequest{
		Pit:   pit,
		Query: query,
	}
	sort := []string{"_shard_doc:asc"}

	pitId, err := fn.search(ctx, searchRequest, sort, emit)
	if err != nil {
		return fmt.Errorf("error searching: %v", err)
	}

	err = fn.closePIT(ctx, pitId)
	if err != nil {
		return fmt.Errorf("error closing Point In Time: %v", err)
	}
	return nil
}

func (fn *readFn) search(
	ctx context.Context,
	searchRequest *SearchRequest,
	sort []string,
	emit func(beam.X),
) (string, error) {
	body := esutil.NewJSONReader(searchRequest)
	response, err := fn.client.Search(
		fn.client.Search.WithContext(ctx),
		fn.client.Search.WithBody(body),
		fn.client.Search.WithSize(fn.BatchSize),
		fn.client.Search.WithSort(sort...),
		fn.client.Search.WithTrackTotalHits(false),
	)
	if err != nil {
		return "", fmt.Errorf("error calling Search API: %v", err)
	}

	defer response.Body.Close()
	if response.IsError() {
		return "", fmt.Errorf("error in response: %v", response.String())
	}

	searchResponse := new(SearchResponse)
	err = json.NewDecoder(response.Body).Decode(searchResponse)
	if err != nil {
		return "", fmt.Errorf("error parsing response body: %v", err)
	}

	hits := searchResponse.Hits.Hits
	for _, hit := range hits {
		out := reflect.New(fn.Type.T).Interface()
		err := json.Unmarshal(hit.Source, out)
		if err != nil {
			return "", fmt.Errorf("error unmarshaling document: %v", err)
		}

		newElem := reflect.ValueOf(out).Elem().Interface()
		emit(newElem)
	}

	hitCount := len(hits)
	pitId := searchResponse.PitId

	if hitCount != fn.BatchSize {
		return pitId, nil
	}

	query := searchRequest.Query
	pit := &PointInTime{
		Id:        pitId,
		KeepAlive: fn.KeepAlive,
	}
	searchAfter := searchResponse.Hits.Hits[hitCount-1].Sort

	nextRequest := &SearchRequest{
		Query:       query,
		Pit:         pit,
		SearchAfter: searchAfter,
	}

	return fn.search(ctx, nextRequest, sort, emit)
}

func (fn *readFn) openPIT(ctx context.Context) (*OpenPITResponse, error) {
	response, err := fn.client.OpenPointInTime(
		[]string{fn.Index},
		fn.KeepAlive,
		fn.client.OpenPointInTime.WithContext(ctx),
	)
	if err != nil {
		return nil, fmt.Errorf("error calling PIT API: %v", err)
	}

	defer response.Body.Close()
	if response.IsError() {
		return nil, fmt.Errorf("error in response: %v", response.String())
	}

	openPitResponse := new(OpenPITResponse)
	err = json.NewDecoder(response.Body).Decode(openPitResponse)
	if err != nil {
		return nil, fmt.Errorf("error parsing response body: %v", err)
	}

	return openPitResponse, nil
}

func (fn *readFn) closePIT(
	ctx context.Context,
	pidId string,
) error {
	data := &OpenPITResponse{Id: pidId}
	body := esutil.NewJSONReader(data)

	response, err := fn.client.ClosePointInTime(
		fn.client.ClosePointInTime.WithContext(ctx),
		fn.client.ClosePointInTime.WithBody(body),
	)
	if err != nil {
		return fmt.Errorf("error calling PIT API: %v", err)
	}

	defer response.Body.Close()
	if response.IsError() {
		return fmt.Errorf("error in response: %v", response.String())
	}
	return nil
}
