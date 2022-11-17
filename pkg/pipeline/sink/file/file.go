package file

import (
	"fmt"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"

	"github.com/johannaojeling/go-beam-pipeline/pkg/io/avroio"
	"github.com/johannaojeling/go-beam-pipeline/pkg/io/csvio"
	"github.com/johannaojeling/go-beam-pipeline/pkg/io/jsonio"
	"github.com/johannaojeling/go-beam-pipeline/pkg/io/parquetio"
	"github.com/johannaojeling/go-beam-pipeline/pkg/pipeline/sink/file/avro"
)

type File struct {
	Format Format     `yaml:"format"`
	Path   string     `yaml:"path"`
	Avro   *avro.Avro `yaml:"avro"`
}

func (file *File) Write(scope beam.Scope, col beam.PCollection) error {
	scope = scope.Scope("Write to file")

	var err error

	switch file.Format {
	case AVRO:
		cfg := avroio.WriteConfig{
			Path:   file.Path,
			Schema: file.Avro.Schema,
		}
		avroio.Write(scope, cfg, col)
	case CSV:
		csvio.Write(scope, file.Path, col)
	case JSON:
		jsonio.Write(scope, file.Path, col)
	case PARQUET:
		parquetio.Write(scope, file.Path, col)
	default:
		err = fmt.Errorf("file format %q is not supported", file.Format)
	}

	return err
}
