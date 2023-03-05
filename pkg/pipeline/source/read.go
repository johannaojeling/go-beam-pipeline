package source

import (
	"context"
	"fmt"
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/johannaojeling/go-beam-pipeline/pkg/options"
	"github.com/johannaojeling/go-beam-pipeline/pkg/utils/gcp"
)

func Read(
	ctx context.Context,
	scope beam.Scope,
	opt options.SourceOption,
	secretReader *gcp.SecretReader,
	elemType reflect.Type,
) (beam.PCollection, error) {
	scope = scope.Scope("Read from source")

	var (
		col beam.PCollection
		err error
	)

	switch opt.Format {
	case options.BigQuery:
		col = ReadFromBigQuery(scope, opt.BigQuery, elemType)
	case options.Database:
		col, err = ReadFromDatabase(ctx, scope, opt.Database, secretReader, elemType)
	case options.Elasticsearch:
		col, err = ReadFromElasticsearch(
			ctx,
			scope,
			opt.Elasticsearch,
			secretReader,
			elemType,
		)
	case options.File:
		col, err = ReadFromFile(scope, opt.File, elemType)
	case options.Firestore:
		col = ReadFromFirestore(scope, opt.Firestore, elemType)
	case options.MongoDB:
		col, err = ReadFromMongoDB(ctx, scope, opt.MongoDB, secretReader, elemType)
	case options.Redis:
		col, err = ReadFromRedis(ctx, scope, opt.Redis, secretReader, elemType)
	default:
		err = fmt.Errorf("source format %q not supported", opt.Format)
	}

	return col, err
}
