package mongodb

import (
	"context"
	"fmt"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"

	"github.com/johannaojeling/go-beam-pipeline/pkg/io/mongodbio"
	"github.com/johannaojeling/go-beam-pipeline/pkg/utils/creds"
	"github.com/johannaojeling/go-beam-pipeline/pkg/utils/gcp"
)

type MongoDB struct {
	URL        creds.Credential `yaml:"url"`
	Database   string           `yaml:"database"`
	Collection string           `yaml:"collection"`
	BatchSize  int              `yaml:"batch_size"`
}

func (mongodb *MongoDB) Write(
	ctx context.Context,
	secretReader *gcp.SecretReader,
	scope beam.Scope,
	col beam.PCollection,
) error {
	scope = scope.Scope("Write to MongoDB")

	url, err := mongodb.URL.GetValue(ctx, secretReader)
	if err != nil {
		return fmt.Errorf("error getting URL value: %w", err)
	}

	cfg := mongodbio.WriteConfig{
		URL:        url,
		Database:   mongodb.Database,
		Collection: mongodb.Collection,
		BatchSize:  mongodb.BatchSize,
	}
	mongodbio.Write(scope, cfg, col)

	return nil
}
