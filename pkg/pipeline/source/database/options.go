package database

import (
	"context"
	"fmt"
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/databaseio"
	_ "github.com/lib/pq"

	"github.com/johannaojeling/go-beam-pipeline/pkg/utils/gcp"
)

type Database struct {
	Driver string `yaml:"driver"`
	Dsn    DSN    `yaml:"dsn"`
	Table  string `yaml:"table"`
}

type DSN struct {
	Value  string `yaml:"value"`
	Secret string `yaml:"secret"`
}

func (database Database) Read(
	scope beam.Scope,
	elemType reflect.Type) (beam.PCollection, error) {
	var dsnValue string

	if database.Dsn.Secret != "" {
		var err error
		dsnValue, err = gcp.ReadSecret(context.Background(), database.Dsn.Secret)
		if err != nil {
			return beam.PCollection{}, fmt.Errorf("error retrieving DSN secret: %v", err)
		}
	} else {
		dsnValue = database.Dsn.Value
	}

	return databaseio.Read(scope, database.Driver, dsnValue, database.Table, elemType), nil
}
