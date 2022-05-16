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
	DSN    DSN    `yaml:"dsn"`
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

	if secret := database.DSN.Secret; secret != "" {
		var err error
		dsnValue, err = gcp.ReadSecret(context.Background(), secret)
		if err != nil {
			return beam.PCollection{}, fmt.Errorf("error retrieving DSN secret: %v", err)
		}
	} else {
		dsnValue = database.DSN.Value
	}

	return databaseio.Read(scope, database.Driver, dsnValue, database.Table, elemType), nil
}
