package sink

import (
	"context"
	"fmt"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/databaseio"
	"github.com/johannaojeling/go-beam-pipeline/pkg/options"
	"github.com/johannaojeling/go-beam-pipeline/pkg/utils/gcp"
)

func WriteToDatabase(
	ctx context.Context,
	scope beam.Scope,
	opt options.DatabaseWriteOption,
	secretReader *gcp.SecretReader,
	col beam.PCollection,
) error {
	scope = scope.Scope("Write to database")

	dsn, err := opt.DSN.GetValue(ctx, secretReader)
	if err != nil {
		return fmt.Errorf("error getting DSN value: %w", err)
	}

	databaseio.Write(scope, opt.Driver, dsn, opt.Table, opt.Columns, col)

	return nil
}
