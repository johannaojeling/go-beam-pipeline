package file

import (
	"fmt"
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/parquetio"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/avroio"
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
	switch format := file.Format; format {
	case Avro:
		return avroio.Read(scope, file.Path, elemType), nil
	case Csv:
		return csvio.Read(scope, file.Path, elemType), nil
	case Json:
		return jsonio.Read(scope, file.Path, elemType), nil
	case Parquet:
		return parquetio.Read(scope, file.Path, elemType), nil
	default:
		return beam.PCollection{}, fmt.Errorf("file format %q is not supported", format)
	}
}
