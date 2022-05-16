package avroio

import (
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/avroio"

	"github.com/johannaojeling/go-beam-pipeline/pkg/io/jsonio"
	"github.com/johannaojeling/go-beam-pipeline/pkg/io/stringio"
)

func Write(scope beam.Scope, outputPath string, schema string, col beam.PCollection) {
	scope = scope.Scope("Write to avro")
	elemType := col.Type().Type()
	marshaled := beam.ParDo(scope, &jsonio.MarshalFn{Type: beam.EncodedType{T: elemType}}, col)
	output := beam.ParDo(scope, &stringio.DecodeFn{}, marshaled)
	avroio.Write(scope, outputPath, schema, output)
}
