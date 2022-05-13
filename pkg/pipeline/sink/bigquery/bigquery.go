package bigquery

import (
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/bigqueryio"
)

type BigQuery struct {
	Project string `yaml:"project"`
	Dataset string `yaml:"dataset"`
	Table   string `yaml:"table"`
}

func (bigquery BigQuery) Write(scope beam.Scope, col beam.PCollection) {
	scope = scope.Scope("Write to BigQuery")
	tableName := bigqueryio.QualifiedTableName{
		Project: bigquery.Project,
		Dataset: bigquery.Dataset,
		Table:   bigquery.Table,
	}
	bigqueryio.Write(scope, bigquery.Project, tableName.String(), col)
}
