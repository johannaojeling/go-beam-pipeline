package file

import (
	"fmt"
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/avroio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/parquetio"

	"github.com/johannaojeling/go-beam-pipeline/pkg/io/csvio"
	"github.com/johannaojeling/go-beam-pipeline/pkg/io/jsonio"
)

type File struct {
	Format Format `yaml:"format"`
	Path   string `yaml:"path"`
}

func (file *File) Read(
	scope beam.Scope,
	elemType reflect.Type,
) (beam.PCollection, error) {
	scope = scope.Scope("Read from file")

	var (
		col beam.PCollection
		err error
	)

	switch file.Format {
	case AVRO:
		col = avroio.Read(scope, file.Path, elemType)
	case CSV:
		col = csvio.Read(scope, file.Path, elemType)
	case JSON:
		col = jsonio.Read(scope, file.Path, elemType)
	case PARQUET:
		col = parquetio.Read(scope, file.Path, elemType)
	default:
		err = fmt.Errorf("file format %q is not supported", file.Format)
	}

	return col, err
}
