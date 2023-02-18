package mongodb

import (
	"context"
	"fmt"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/mongodbio"

	"github.com/johannaojeling/go-beam-pipeline/pkg/utils/creds"
	"github.com/johannaojeling/go-beam-pipeline/pkg/utils/gcp"
)

type MongoDB struct {
	URL        creds.Credential `yaml:"url"`
	Database   string           `yaml:"database"`
	Collection string           `yaml:"collection"`
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

	mongodbio.Write(
		scope,
		url,
		mongodb.Database,
		mongodb.Collection,
		col,
	)

	return nil
}
