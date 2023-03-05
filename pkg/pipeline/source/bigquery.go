package source

import (
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/bigqueryio"
	"github.com/johannaojeling/go-beam-pipeline/pkg/options"
)

func ReadFromBigQuery(
	scope beam.Scope,
	opt options.BigQueryReadOption,
	elemType reflect.Type,
) beam.PCollection {
	scope = scope.Scope("Read from BigQuery")
	tableName := bigqueryio.QualifiedTableName{
		Project: opt.Project,
		Dataset: opt.Dataset,
		Table:   opt.Table,
	}

	return bigqueryio.Read(scope, opt.Project, tableName.String(), elemType)
}
