package sink

import (
	"fmt"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/johannaojeling/go-beam-pipeline/pkg/io/avroio"
	"github.com/johannaojeling/go-beam-pipeline/pkg/io/csvio"
	"github.com/johannaojeling/go-beam-pipeline/pkg/io/jsonio"
	"github.com/johannaojeling/go-beam-pipeline/pkg/io/parquetio"
	"github.com/johannaojeling/go-beam-pipeline/pkg/options"
)

func WriteToFile(scope beam.Scope, opt options.FileWriteOption, col beam.PCollection) error {
	scope = scope.Scope("Write to file")

	var err error

	switch opt.Format {
	case options.Avro:
		cfg := avroio.WriteConfig{
			Path:   opt.Path,
			Schema: opt.Avro.Schema,
		}
		avroio.Write(scope, cfg, col)
	case options.CSV:
		csvio.Write(scope, opt.Path, col)
	case options.JSON:
		jsonio.Write(scope, opt.Path, col)
	case options.Parquet:
		parquetio.Write(scope, opt.Path, col)
	default:
		err = fmt.Errorf("file format %q not supported", opt.Format)
	}

	return err
}
