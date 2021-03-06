package jsonio

import (
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/textio"

	"github.com/johannaojeling/go-beam-pipeline/pkg/io/stringio"
)

func Write(scope beam.Scope, path string, col beam.PCollection) {
	scope = scope.Scope("Write to json")
	elemType := col.Type().Type()
	marshaled := beam.ParDo(scope, NewMarshalFn(elemType), col)
	output := beam.ParDo(scope, stringio.NewDecodeFn(), marshaled)
	textio.Write(scope, path, output)
}
