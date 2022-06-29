package elasticsearchio

import (
	"fmt"
	"reflect"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/cenkalti/backoff/v4"
	"github.com/elastic/go-elasticsearch/v8"
)

func init() {
	beam.RegisterType(reflect.TypeOf((*esFn)(nil)))
}

type esFn struct {
	Addresses []string
	CloudId   string
	ApiKey    string
	Index     string
	Type      beam.EncodedType
	client    *elasticsearch.Client
}

func (fn *esFn) Setup() error {
	client, err := newClient(fn.Addresses, fn.CloudId, fn.ApiKey)
	if err != nil {
		return fmt.Errorf("error initializing Elasticsearch client: %v", err)
	}
	fn.client = client
	return nil
}

func newClient(addresses []string, cloudId string, apiKey string) (*elasticsearch.Client, error) {
	retryBackoff := backoff.NewExponentialBackOff()
	retryBackoffFn := func(i int) time.Duration {
		if i == 1 {
			retryBackoff.Reset()
		}
		return retryBackoff.NextBackOff()
	}

	cfg := elasticsearch.Config{
		Addresses:     addresses,
		CloudID:       cloudId,
		APIKey:        apiKey,
		RetryOnStatus: []int{502, 503, 504, 429},
		RetryBackoff:  retryBackoffFn,
		MaxRetries:    5,
	}

	client, err := elasticsearch.NewClient(cfg)
	if err != nil {
		return nil, fmt.Errorf("error initializing client")
	}
	return client, nil
}
