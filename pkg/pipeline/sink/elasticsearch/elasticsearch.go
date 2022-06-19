package elasticsearch

import (
	"fmt"
	"strings"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"

	"github.com/johannaojeling/go-beam-pipeline/pkg/io/elasticsearchio"
	"github.com/johannaojeling/go-beam-pipeline/pkg/utils/creds"
)

type Elasticsearch struct {
	URLs       creds.Credential `yaml:"urls"`
	CloudId    creds.Credential `yaml:"cloud_id"`
	ApiKey     creds.Credential `yaml:"api_key"`
	Index      string           `yaml:"index"`
	FlushBytes int              `yaml:"flush_bytes"`
}

func (es Elasticsearch) Write(scope beam.Scope, col beam.PCollection) error {
	scope = scope.Scope("Write to Elasticsearch")

	urls, err := es.URLs.GetValue()
	if err != nil {
		return fmt.Errorf("failed to get URLs value: %v", err)
	}
	var addresses []string
	if urls != "" {
		addresses = strings.Split(urls, ",")
	}

	cloudId, err := es.CloudId.GetValue()
	if err != nil {
		return fmt.Errorf("failed to get Cloud ID value: %v", err)
	}

	apiKey, err := es.ApiKey.GetValue()
	if err != nil {
		return fmt.Errorf("failed to get API key value: %v", err)
	}

	elasticsearchio.Write(scope, addresses, cloudId, apiKey, es.Index, es.FlushBytes, col)
	return nil
}
