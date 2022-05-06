package file

import (
	"fmt"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"

	"github.com/johannaojeling/go-beam-pipeline/pkg/io/avroio"
	"github.com/johannaojeling/go-beam-pipeline/pkg/io/csvio"
	"github.com/johannaojeling/go-beam-pipeline/pkg/io/jsonio"
	"github.com/johannaojeling/go-beam-pipeline/pkg/pipeline/sink/file/avro"
)

type File struct {
	Format Format    `yaml:"format"`
	Path   string    `yaml:"path"`
	Avro   avro.Avro `yaml:"avro"`
}

func (file File) Write(scope beam.Scope, col beam.PCollection) error {
	scope = scope.Scope("Write to file")
	switch format := file.Format; format {
	case Avro:
		avroio.Write(scope, file.Path, file.Avro.Schema, col)
	case Csv:
		csvio.Write(scope, file.Path, col)
	case Json:
		jsonio.Write(scope, file.Path, col)
	default:
		return fmt.Errorf("file format %q is not supported", format)
	}
	return nil
}
