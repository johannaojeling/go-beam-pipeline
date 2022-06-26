package sink

import (
	"context"
	"fmt"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"

	"github.com/johannaojeling/go-beam-pipeline/pkg/pipeline/sink/bigquery"
	"github.com/johannaojeling/go-beam-pipeline/pkg/pipeline/sink/elasticsearch"
	"github.com/johannaojeling/go-beam-pipeline/pkg/pipeline/sink/file"
	"github.com/johannaojeling/go-beam-pipeline/pkg/pipeline/sink/firestore"
	"github.com/johannaojeling/go-beam-pipeline/pkg/pipeline/sink/redis"
	"github.com/johannaojeling/go-beam-pipeline/pkg/utils/gcp"
)

type Sink struct {
	Format        Format                      `yaml:"format"`
	BigQuery      bigquery.BigQuery           `yaml:"bigquery"`
	Elasticsearch elasticsearch.Elasticsearch `yaml:"elasticsearch"`
	File          file.File                   `yaml:"file"`
	Firestore     firestore.Firestore         `yaml:"firestore"`
	Redis         redis.Redis                 `yaml:"redis"`
}

func (sink Sink) Write(
	ctx context.Context,
	secretReader *gcp.SecretReader,
	scope beam.Scope,
	col beam.PCollection,
) error {
	scope = scope.Scope("Write to sink")
	switch format := sink.Format; format {
	case BigQuery:
		sink.BigQuery.Write(scope, col)
		return nil
	case Elasticsearch:
		return sink.Elasticsearch.Write(ctx, secretReader, scope, col)
	case File:
		return sink.File.Write(scope, col)
	case Firestore:
		sink.Firestore.Write(scope, col)
		return nil
	case Redis:
		return sink.Redis.Write(ctx, secretReader, scope, col)
	default:
		return fmt.Errorf("sink format %q is not supported", format)
	}
}
