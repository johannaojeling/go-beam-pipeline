package sink

import (
	"context"
	"fmt"
	"strings"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/johannaojeling/go-beam-pipeline/pkg/io/elasticsearchio"
	"github.com/johannaojeling/go-beam-pipeline/pkg/options"
	"github.com/johannaojeling/go-beam-pipeline/pkg/utils/gcp"
)

func WriteToElasticsearch(
	ctx context.Context,
	scope beam.Scope,
	opt options.ElasticsearchWriteOption,
	secretReader *gcp.SecretReader,
	col beam.PCollection,
) error {
	scope = scope.Scope("Write to Elasticsearch")

	urls, err := opt.URLs.GetValue(ctx, secretReader)
	if err != nil {
		return fmt.Errorf("error getting URLs value: %w", err)
	}

	var addresses []string

	if urls != "" {
		addresses = strings.Split(urls, ",")
	}

	cloudID, err := opt.CloudID.GetValue(ctx, secretReader)
	if err != nil {
		return fmt.Errorf("error getting Cloud ID value: %w", err)
	}

	apiKey, err := opt.APIKey.GetValue(ctx, secretReader)
	if err != nil {
		return fmt.Errorf("error getting API key value: %w", err)
	}

	cfg := elasticsearchio.WriteConfig{
		Addresses:  addresses,
		CloudID:    cloudID,
		APIKey:     apiKey,
		Index:      opt.Index,
		FlushBytes: opt.FlushBytes,
	}
	elasticsearchio.Write(scope, cfg, col)

	return nil
}
