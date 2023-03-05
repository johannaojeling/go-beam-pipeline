package sink

import (
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/bigqueryio"
	"github.com/johannaojeling/go-beam-pipeline/pkg/options"
)

func WriteToBigQuery(scope beam.Scope, opt options.BigQueryWriteOption, col beam.PCollection) {
	scope = scope.Scope("Write to BigQuery")
	tableName := bigqueryio.QualifiedTableName{
		Project: opt.Project,
		Dataset: opt.Dataset,
		Table:   opt.Table,
	}
	bigqueryio.Write(scope, opt.Project, tableName.String(), col)
}
