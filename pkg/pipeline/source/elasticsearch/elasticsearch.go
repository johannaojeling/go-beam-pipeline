package elasticsearch

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"

	"github.com/johannaojeling/go-beam-pipeline/pkg/io/elasticsearchio"
	"github.com/johannaojeling/go-beam-pipeline/pkg/utils/creds"
	"github.com/johannaojeling/go-beam-pipeline/pkg/utils/gcp"
)

type Elasticsearch struct {
	URLs      creds.Credential `yaml:"urls"`
	CloudId   creds.Credential `yaml:"cloud_id"`
	ApiKey    creds.Credential `yaml:"api_key"`
	Index     string           `yaml:"index"`
	Query     string           `yaml:"query"`
	BatchSize int              `yaml:"batch_size"`
	KeepAlive string           `yaml:"keep_alive"`
}

func (es Elasticsearch) Read(
	ctx context.Context,
	secretReader *gcp.SecretReader,
	scope beam.Scope,
	elemType reflect.Type) (beam.PCollection, error) {
	scope = scope.Scope("Read from Elasticsearch")

	urls, err := es.URLs.GetValue(ctx, secretReader)
	if err != nil {
		return beam.PCollection{}, fmt.Errorf("error getting URLs value: %v", err)
	}
	var addresses []string
	if urls != "" {
		addresses = strings.Split(urls, ",")
	}

	cloudId, err := es.CloudId.GetValue(ctx, secretReader)
	if err != nil {
		return beam.PCollection{}, fmt.Errorf("error getting Cloud ID value: %v", err)
	}

	apiKey, err := es.ApiKey.GetValue(ctx, secretReader)
	if err != nil {
		return beam.PCollection{}, fmt.Errorf("error getting API key value: %v", err)
	}

	cfg := elasticsearchio.ReadConfig{
		Addresses: addresses,
		CloudId:   cloudId,
		ApiKey:    apiKey,
		Index:     es.Index,
		Query:     es.Query,
		BatchSize: es.BatchSize,
		KeepAlive: es.KeepAlive,
	}

	return elasticsearchio.Read(scope, cfg, elemType), nil
}
