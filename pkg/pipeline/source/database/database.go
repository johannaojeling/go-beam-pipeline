package database

import (
	"fmt"
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/databaseio"
	_ "github.com/lib/pq"

	"github.com/johannaojeling/go-beam-pipeline/pkg/utils/creds"
)

type Database struct {
	Driver string           `yaml:"driver"`
	DSN    creds.Credential `yaml:"dsn"`
	Table  string           `yaml:"table"`
}

func (database Database) Read(
	scope beam.Scope,
	elemType reflect.Type,
) (beam.PCollection, error) {
	dsn, err := database.DSN.GetValue()
	if err != nil {
		return beam.PCollection{}, fmt.Errorf("failed to get DSN value: %v", err)
	}

	return databaseio.Read(scope, database.Driver, dsn, database.Table, elemType), nil
}
