package source

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/johannaojeling/go-beam-pipeline/pkg/io/elasticsearchio"
	"github.com/johannaojeling/go-beam-pipeline/pkg/options"
	"github.com/johannaojeling/go-beam-pipeline/pkg/utils/gcp"
)

func ReadFromElasticsearch(
	ctx context.Context,
	scope beam.Scope,
	opt options.ElasticsearchReadOption,
	secretReader *gcp.SecretReader,
	elemType reflect.Type,
) (beam.PCollection, error) {
	scope = scope.Scope("Read from Elasticsearch")

	urls, err := opt.URLs.GetValue(ctx, secretReader)
	if err != nil {
		return beam.PCollection{}, fmt.Errorf("error getting URLs value: %w", err)
	}

	var addresses []string
	if urls != "" {
		addresses = strings.Split(urls, ",")
	}

	cloudID, err := opt.CloudID.GetValue(ctx, secretReader)
	if err != nil {
		return beam.PCollection{}, fmt.Errorf("error getting Cloud ID value: %w", err)
	}

	apiKey, err := opt.APIKey.GetValue(ctx, secretReader)
	if err != nil {
		return beam.PCollection{}, fmt.Errorf("error getting API key value: %w", err)
	}

	cfg := elasticsearchio.ReadConfig{
		Addresses: addresses,
		CloudID:   cloudID,
		APIKey:    apiKey,
		Index:     opt.Index,
		Query:     opt.Query,
		BatchSize: opt.BatchSize,
		KeepAlive: opt.KeepAlive,
	}

	return elasticsearchio.Read(scope, cfg, elemType), nil
}
