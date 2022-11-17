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
	CloudID    creds.Credential `yaml:"cloud_id"`
	APIKey     creds.Credential `yaml:"api_key"`
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
		return fmt.Errorf("error getting URLs value: %w", err)
	}

	var addresses []string

	if urls != "" {
		addresses = strings.Split(urls, ",")
	}

	cloudID, err := es.CloudID.GetValue(ctx, secretReader)
	if err != nil {
		return fmt.Errorf("error getting Cloud ID value: %w", err)
	}

	apiKey, err := es.APIKey.GetValue(ctx, secretReader)
	if err != nil {
		return fmt.Errorf("error getting API key value: %w", err)
	}

	cfg := elasticsearchio.WriteConfig{
		Addresses:  addresses,
		CloudID:    cloudID,
		APIKey:     apiKey,
		Index:      es.Index,
		FlushBytes: es.FlushBytes,
	}
	elasticsearchio.Write(scope, cfg, col)

	return nil
}
