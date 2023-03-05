package source

import (
	"context"
	"fmt"
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/mongodbio"
	"github.com/johannaojeling/go-beam-pipeline/pkg/options"
	"github.com/johannaojeling/go-beam-pipeline/pkg/utils/gcp"
	"go.mongodb.org/mongo-driver/bson"
)

func ReadFromMongoDB(
	ctx context.Context,
	scope beam.Scope,
	opt options.MongoDBReadOption,
	secretReader *gcp.SecretReader,
	elemType reflect.Type,
) (beam.PCollection, error) {
	scope = scope.Scope("Read from MongoDB")

	url, err := opt.URL.GetValue(ctx, secretReader)
	if err != nil {
		return beam.PCollection{}, fmt.Errorf("error getting URL value: %w", err)
	}

	var filter bson.M

	if opt.Filter != "" {
		if err := bson.UnmarshalExtJSON([]byte(opt.Filter), true, &filter); err != nil {
			return beam.PCollection{}, fmt.Errorf("error unmarshalling filter: %w", err)
		}
	}

	return mongodbio.Read(
		scope,
		url,
		opt.Database,
		opt.Collection,
		elemType,
		mongodbio.WithReadFilter(filter),
		mongodbio.WithReadBucketAuto(true),
	), nil
}
