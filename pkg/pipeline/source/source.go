package source

import (
	"context"
	"fmt"
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"

	"github.com/johannaojeling/go-beam-pipeline/pkg/pipeline/source/bigquery"
	"github.com/johannaojeling/go-beam-pipeline/pkg/pipeline/source/database"
	"github.com/johannaojeling/go-beam-pipeline/pkg/pipeline/source/elasticsearch"
	"github.com/johannaojeling/go-beam-pipeline/pkg/pipeline/source/file"
	"github.com/johannaojeling/go-beam-pipeline/pkg/pipeline/source/firestore"
	"github.com/johannaojeling/go-beam-pipeline/pkg/pipeline/source/mongodb"
	"github.com/johannaojeling/go-beam-pipeline/pkg/pipeline/source/redis"
	"github.com/johannaojeling/go-beam-pipeline/pkg/utils/gcp"
)

type Source struct {
	Format        Format                       `yaml:"format"`
	BigQuery      *bigquery.BigQuery           `yaml:"bigquery"`
	Database      *database.Database           `yaml:"database"`
	Elasticsearch *elasticsearch.Elasticsearch `yaml:"elasticsearch"`
	File          *file.File                   `yaml:"file"`
	Firestore     *firestore.Firestore         `yaml:"firestore"`
	MongoDB       *mongodb.MongoDB             `yaml:"mongodb"`
	Redis         *redis.Redis                 `yaml:"redis"`
}

func (source *Source) Read(
	ctx context.Context,
	secretReader *gcp.SecretReader,
	scope beam.Scope,
	elemType reflect.Type,
) (beam.PCollection, error) {
	scope = scope.Scope("Read from source")

	var (
		col beam.PCollection
		err error
	)

	switch source.Format {
	case BigQuery:
		col = source.BigQuery.Read(scope, elemType)
	case Database:
		col, err = source.Database.Read(ctx, secretReader, scope, elemType)
	case Elasticsearch:
		col, err = source.Elasticsearch.Read(ctx, secretReader, scope, elemType)
	case File:
		col, err = source.File.Read(scope, elemType)
	case Firestore:
		col = source.Firestore.Read(scope, elemType)
	case MongoDB:
		col, err = source.MongoDB.Read(ctx, secretReader, scope, elemType)
	case Redis:
		col, err = source.Redis.Read(ctx, secretReader, scope, elemType)
	default:
		err = fmt.Errorf("source format %q is not supported", source.Format)
	}

	return col, err
}
