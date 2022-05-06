package bigquery

import (
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/bigqueryio"
)

type BigQuery struct {
	ProjectId string `yaml:"project_id"`
	Table     string `yaml:"table"`
}

func (bigquery BigQuery) Read(
	scope beam.Scope,
	elemType reflect.Type,
) beam.PCollection {
	scope = scope.Scope("Read from BigQuery")
	return bigqueryio.Read(scope, bigquery.ProjectId, bigquery.Table, elemType)
}
