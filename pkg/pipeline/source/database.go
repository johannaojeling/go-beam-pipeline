package source

import (
	"context"
	"fmt"
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/databaseio"
	"github.com/johannaojeling/go-beam-pipeline/pkg/options"
	"github.com/johannaojeling/go-beam-pipeline/pkg/utils/gcp"
)

func ReadFromDatabase(
	ctx context.Context,
	scope beam.Scope,
	opt options.DatabaseReadOption,
	secretReader *gcp.SecretReader,
	elemType reflect.Type,
) (beam.PCollection, error) {
	scope = scope.Scope("Read from database")

	dsn, err := opt.DSN.GetValue(ctx, secretReader)
	if err != nil {
		return beam.PCollection{}, fmt.Errorf("error getting DSN value: %w", err)
	}

	return databaseio.Read(scope, opt.Driver, dsn, opt.Table, elemType), nil
}
