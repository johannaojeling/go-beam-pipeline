package elasticsearch

import (
	"context"
	"fmt"
	"strings"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"

	"github.com/johannaojeling/go-beam-pipeline/pkg/io/elasticsearchio"
	"github.com/johannaojeling/go-beam-pipeline/pkg/utils/creds"
	"github.com/johannaojeling/go-beam-pipeline/pkg/utils/gcp"
)

type Elasticsearch struct {
	URLs       creds.Credential `yaml:"urls"`
	CloudId    creds.Credential `yaml:"cloud_id"`
	ApiKey     creds.Credential `yaml:"api_key"`
	Index      string           `yaml:"index"`
	FlushBytes int              `yaml:"flush_bytes"`
}

func (es *Elasticsearch) Write(
	ctx context.Context,
	secretReader *gcp.SecretReader,
	scope beam.Scope,
	col beam.PCollection,
) error {
	scope = scope.Scope("Write to Elasticsearch")

	urls, err := es.URLs.GetValue(ctx, secretReader)
	if err != nil {
		return fmt.Errorf("error getting URLs value: %v", err)
	}
	var addresses []string
	if urls != "" {
		addresses = strings.Split(urls, ",")
	}

	cloudId, err := es.CloudId.GetValue(ctx, secretReader)
	if err != nil {
		return fmt.Errorf("error getting Cloud ID value: %v", err)
	}

	apiKey, err := es.ApiKey.GetValue(ctx, secretReader)
	if err != nil {
		return fmt.Errorf("error getting API key value: %v", err)
	}

	cfg := elasticsearchio.WriteConfig{
		Addresses:  addresses,
		CloudId:    cloudId,
		ApiKey:     apiKey,
		Index:      es.Index,
		FlushBytes: es.FlushBytes,
	}
	elasticsearchio.Write(scope, cfg, col)
	return nil
}
