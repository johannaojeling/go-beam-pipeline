package database

import (
	"context"
	"fmt"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/databaseio"

	"github.com/johannaojeling/go-beam-pipeline/pkg/utils/creds"
	"github.com/johannaojeling/go-beam-pipeline/pkg/utils/gcp"
)

type Database struct {
	Driver  string           `yaml:"driver"`
	DSN     creds.Credential `yaml:"dsn"`
	Table   string           `yaml:"table"`
	Columns []string         `yaml:"columns"`
}

func (db *Database) Write(
	ctx context.Context,
	secretReader *gcp.SecretReader,
	scope beam.Scope,
	col beam.PCollection,
) error {
	scope = scope.Scope("Write to db")

	dsn, err := db.DSN.GetValue(ctx, secretReader)
	if err != nil {
		return fmt.Errorf("error getting DSN value: %w", err)
	}

	columns := db.Columns
	if columns == nil {
		columns = []string{}
	}

	databaseio.Write(scope, db.Driver, dsn, db.Table, columns, col)

	return nil
}
