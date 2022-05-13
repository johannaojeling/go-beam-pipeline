package bigquery

import (
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/bigqueryio"
)

type BigQuery struct {
	Project string `yaml:"project"`
	Table   string `yaml:"table"`
}

func (bigquery BigQuery) Write(scope beam.Scope, col beam.PCollection) {
	scope = scope.Scope("Write to BigQuery")
	bigqueryio.Write(scope, bigquery.Project, bigquery.Table, col)
}
