package parquetio

import (
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/parquetio"
)

func Write(scope beam.Scope, outputPath string, col beam.PCollection) {
	scope = scope.Scope("Write to parquet")
	elemType := col.Type().Type()
	parquetio.Write(scope, outputPath, elemType, col)
}
