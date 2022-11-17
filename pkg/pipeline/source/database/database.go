package database

import (
	"context"
	"fmt"
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/databaseio"

	"github.com/johannaojeling/go-beam-pipeline/pkg/utils/creds"
	"github.com/johannaojeling/go-beam-pipeline/pkg/utils/gcp"
)

type Database struct {
	Driver string           `yaml:"driver"`
	DSN    creds.Credential `yaml:"dsn"`
	Table  string           `yaml:"table"`
}

func (db *Database) Read(
	ctx context.Context,
	secretReader *gcp.SecretReader,
	scope beam.Scope,
	elemType reflect.Type,
) (beam.PCollection, error) {
	dsn, err := db.DSN.GetValue(ctx, secretReader)
	if err != nil {
		return beam.PCollection{}, fmt.Errorf("error getting DSN value: %w", err)
	}

	return databaseio.Read(scope, db.Driver, dsn, db.Table, elemType), nil
}
