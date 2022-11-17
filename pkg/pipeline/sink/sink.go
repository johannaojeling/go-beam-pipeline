package sink

import (
	"context"
	"fmt"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"

	"github.com/johannaojeling/go-beam-pipeline/pkg/pipeline/sink/bigquery"
	"github.com/johannaojeling/go-beam-pipeline/pkg/pipeline/sink/database"
	"github.com/johannaojeling/go-beam-pipeline/pkg/pipeline/sink/elasticsearch"
	"github.com/johannaojeling/go-beam-pipeline/pkg/pipeline/sink/file"
	"github.com/johannaojeling/go-beam-pipeline/pkg/pipeline/sink/firestore"
	"github.com/johannaojeling/go-beam-pipeline/pkg/pipeline/sink/mongodb"
	"github.com/johannaojeling/go-beam-pipeline/pkg/pipeline/sink/redis"
	"github.com/johannaojeling/go-beam-pipeline/pkg/utils/gcp"
)

type Sink struct {
	Format        Format                       `yaml:"format"`
	BigQuery      *bigquery.BigQuery           `yaml:"bigquery"`
	Database      *database.Database           `yaml:"database"`
	Elasticsearch *elasticsearch.Elasticsearch `yaml:"elasticsearch"`
	File          *file.File                   `yaml:"file"`
	Firestore     *firestore.Firestore         `yaml:"firestore"`
	MongoDB       *mongodb.MongoDB             `yaml:"mongodb"`
	Redis         *redis.Redis                 `yaml:"redis"`
}

func (sink *Sink) Write(
	ctx context.Context,
	secretReader *gcp.SecretReader,
	scope beam.Scope,
	col beam.PCollection,
) error {
	scope = scope.Scope("Write to sink")

	var err error

	switch sink.Format {
	case BigQuery:
		sink.BigQuery.Write(scope, col)
	case Database:
		err = sink.Database.Write(ctx, secretReader, scope, col)
	case Elasticsearch:
		err = sink.Elasticsearch.Write(ctx, secretReader, scope, col)
	case File:
		err = sink.File.Write(scope, col)
	case MongoDB:
		err = sink.MongoDB.Write(ctx, secretReader, scope, col)
	case Redis:
		err = sink.Redis.Write(ctx, secretReader, scope, col)
	default:
		err = fmt.Errorf("sink format %q is not supported", sink.Format)
	}

	return err
}
