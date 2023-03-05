package sink

import (
	"context"
	"fmt"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/mongodbio"
	"github.com/johannaojeling/go-beam-pipeline/pkg/options"
	"github.com/johannaojeling/go-beam-pipeline/pkg/utils/gcp"
)

func WriteToMongoDB(
	ctx context.Context,
	scope beam.Scope,
	opt options.MongoDBWriteOption,
	secretReader *gcp.SecretReader,
	col beam.PCollection,
) error {
	scope = scope.Scope("Write to MongoDB")

	url, err := opt.URL.GetValue(ctx, secretReader)
	if err != nil {
		return fmt.Errorf("error getting URL value: %w", err)
	}

	mongodbio.Write(
		scope,
		url,
		opt.Database,
		opt.Collection,
		col,
	)

	return nil
}
