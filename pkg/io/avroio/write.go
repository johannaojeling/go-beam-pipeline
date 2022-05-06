package avroio

import (
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/avroio"

	"github.com/johannaojeling/go-beam-pipeline/pkg/io/jsonio"
)

func init() {
	beam.RegisterType(reflect.TypeOf((*jsonio.MarshalJsonFn)(nil)))
}

func Write(scope beam.Scope, outputPath string, schema string, col beam.PCollection) {
	scope = scope.Scope("Write to avro")
	elemType := col.Type().Type()
	output := beam.ParDo(scope, &jsonio.MarshalJsonFn{Type: beam.EncodedType{T: elemType}}, col)
	avroio.Write(scope, outputPath, schema, output)
}
