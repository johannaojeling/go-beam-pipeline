package source

import (
	"fmt"
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/avroio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/parquetio"
	"github.com/johannaojeling/go-beam-pipeline/pkg/io/csvio"
	"github.com/johannaojeling/go-beam-pipeline/pkg/io/jsonio"
	"github.com/johannaojeling/go-beam-pipeline/pkg/options"
)

func ReadFromFile(
	scope beam.Scope,
	opt options.FileReadOption,
	elemType reflect.Type,
) (beam.PCollection, error) {
	scope = scope.Scope("Read from file")

	var (
		col beam.PCollection
		err error
	)

	switch opt.Format {
	case options.Avro:
		col = avroio.Read(scope, opt.Path, elemType)
	case options.CSV:
		col = csvio.Read(scope, opt.Path, elemType)
	case options.JSON:
		col = jsonio.Read(scope, opt.Path, elemType)
	case options.Parquet:
		col = parquetio.Read(scope, opt.Path, elemType)
	default:
		err = fmt.Errorf("file format %q not supported", opt.Format)
	}

	return col, err
}
