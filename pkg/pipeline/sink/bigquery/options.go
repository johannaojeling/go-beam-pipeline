package bigquery

import (
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/bigqueryio"
)

type BigQuery struct {
	ProjectId string `yaml:"project_id"`
	Table     string `yaml:"table"`
}

func (bigquery BigQuery) Write(scope beam.Scope, col beam.PCollection) {
	scope = scope.Scope("Write to BigQuery")
	bigqueryio.Write(scope, bigquery.ProjectId, bigquery.Table, col)
}
