package bigquery

import (
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/bigqueryio"
)

type BigQuery struct {
	Project string `yaml:"project"`
	Dataset string `yaml:"dataset"`
	Table   string `yaml:"table"`
}

func (bigquery BigQuery) Read(
	scope beam.Scope,
	elemType reflect.Type,
) beam.PCollection {
	scope = scope.Scope("Read from BigQuery")
	tableName := bigqueryio.QualifiedTableName{
		Project: bigquery.Project,
		Dataset: bigquery.Dataset,
		Table:   bigquery.Table,
	}
	return bigqueryio.Read(scope, bigquery.Project, tableName.String(), elemType)
}
