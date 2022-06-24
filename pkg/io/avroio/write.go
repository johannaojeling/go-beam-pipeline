package avroio

import (
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/avroio"

	"github.com/johannaojeling/go-beam-pipeline/pkg/io/jsonio"
	"github.com/johannaojeling/go-beam-pipeline/pkg/io/stringio"
)

type WriteConfig struct {
	Path   string
	Schema string
}

func Write(scope beam.Scope, cfg WriteConfig, col beam.PCollection) {
	scope = scope.Scope("Write to avro")
	elemType := col.Type().Type()
	marshaled := beam.ParDo(scope, jsonio.NewMarshalFn(elemType), col)
	output := beam.ParDo(scope, stringio.NewDecodeFn(), marshaled)
	avroio.Write(scope, cfg.Path, cfg.Schema, output)
}
