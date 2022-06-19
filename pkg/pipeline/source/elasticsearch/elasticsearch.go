package elasticsearch

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"

	"github.com/johannaojeling/go-beam-pipeline/pkg/io/elasticsearchio"
	"github.com/johannaojeling/go-beam-pipeline/pkg/utils/creds"
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
	scope beam.Scope,
	elemType reflect.Type) (beam.PCollection, error) {
	scope = scope.Scope("Read from Elasticsearch")

	urls, err := es.URLs.GetValue()
	if err != nil {
		return beam.PCollection{}, fmt.Errorf("failed to get URLs value: %v", err)
	}
	var addresses []string
	if urls != "" {
		addresses = strings.Split(urls, ",")
	}

	cloudId, err := es.CloudId.GetValue()
	if err != nil {
		return beam.PCollection{}, fmt.Errorf("failed to get Cloud ID value: %v", err)
	}

	apiKey, err := es.ApiKey.GetValue()
	if err != nil {
		return beam.PCollection{}, fmt.Errorf("failed to get API key value: %v", err)
	}

	return elasticsearchio.Read(
		scope,
		addresses,
		cloudId,
		apiKey,
		es.Index,
		es.Query,
		es.BatchSize,
		es.KeepAlive,
		elemType,
	), nil
}
