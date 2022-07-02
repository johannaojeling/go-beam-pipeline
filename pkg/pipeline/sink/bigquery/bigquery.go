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

func (bq *BigQuery) Write(scope beam.Scope, col beam.PCollection) {
	scope = scope.Scope("Write to BigQuery")
	tableName := bigqueryio.QualifiedTableName{
		Project: bq.Project,
		Dataset: bq.Dataset,
		Table:   bq.Table,
	}
	bigqueryio.Write(scope, bq.Project, tableName.String(), col)
}
