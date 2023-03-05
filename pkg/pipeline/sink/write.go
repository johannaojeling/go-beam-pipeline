package sink

import (
	"context"
	"fmt"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/johannaojeling/go-beam-pipeline/pkg/options"
	"github.com/johannaojeling/go-beam-pipeline/pkg/utils/gcp"
)

func Write(
	ctx context.Context,
	scope beam.Scope,
	opt options.SinkOption,
	secretReader *gcp.SecretReader,
	col beam.PCollection,
) error {
	scope = scope.Scope("Write to sink")

	var err error

	switch opt.Format {
	case options.BigQuery:
		WriteToBigQuery(scope, opt.BigQuery, col)
	case options.Database:
		err = WriteToDatabase(ctx, scope, opt.Database, secretReader, col)
	case options.Elasticsearch:
		err = WriteToElasticsearch(ctx, scope, opt.Elasticsearch, secretReader, col)
	case options.File:
		err = WriteToFile(scope, opt.File, col)
	case options.Firestore:
		WriteToFirestore(scope, opt.Firestore, col)
	case options.MongoDB:
		err = WriteToMongoDB(ctx, scope, opt.MongoDB, secretReader, col)
	case options.Redis:
		err = WriteToRedis(ctx, scope, opt.Redis, secretReader, col)
	default:
		err = fmt.Errorf("sink format %q not supported", opt.Format)
	}

	return err
}
