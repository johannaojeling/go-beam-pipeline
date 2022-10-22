package mongodb

import (
	"context"
	"fmt"
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"

	"github.com/johannaojeling/go-beam-pipeline/pkg/io/mongodbio"
	"github.com/johannaojeling/go-beam-pipeline/pkg/utils/creds"
	"github.com/johannaojeling/go-beam-pipeline/pkg/utils/gcp"
)

type MongoDB struct {
	URL        creds.Credential `yaml:"url"`
	Database   string           `yaml:"database"`
	Collection string           `yaml:"collection"`
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
		return beam.PCollection{}, fmt.Errorf("error getting URL value: %v", err)
	}

	cfg := mongodbio.ReadConfig{
		URL:        url,
		Database:   mongodb.Database,
		Collection: mongodb.Collection,
	}
	return mongodbio.Read(scope, cfg, elemType), nil
}
