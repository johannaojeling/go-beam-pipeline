package mongodb

import (
	"context"
	"fmt"
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/mongodbio"
	"go.mongodb.org/mongo-driver/bson"

	"github.com/johannaojeling/go-beam-pipeline/pkg/utils/creds"
	"github.com/johannaojeling/go-beam-pipeline/pkg/utils/gcp"
)

type MongoDB struct {
	URL        creds.Credential `yaml:"url"`
	Database   string           `yaml:"database"`
	Collection string           `yaml:"collection"`
	Filter     string           `yaml:"filter"`
}

func (mongodb *MongoDB) Read(
	ctx context.Context,
	secretReader *gcp.SecretReader,
	scope beam.Scope,
	elemType reflect.Type,
) (beam.PCollection, error) {
	scope = scope.Scope("Read from MongoDB")

	url, err := mongodb.URL.GetValue(ctx, secretReader)
	if err != nil {
		return beam.PCollection{}, fmt.Errorf("error getting URL value: %w", err)
	}

	var filter bson.M

	if mongodb.Filter != "" {
		if err := bson.UnmarshalExtJSON([]byte(mongodb.Filter), true, &filter); err != nil {
			return beam.PCollection{}, fmt.Errorf("error unmarshalling filter: %w", err)
		}
	}

	return mongodbio.Read(
		scope,
		url,
		mongodb.Database,
		mongodb.Collection,
		elemType,
		mongodbio.WithReadFilter(filter),
		mongodbio.WithReadBucketAuto(true),
	), nil
}
