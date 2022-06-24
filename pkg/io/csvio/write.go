package csvio

import (
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/textio"
)

func Write(scope beam.Scope, filename string, col beam.PCollection) {
	scope = scope.Scope("Write to csv")
	elemType := col.Type().Type()
	output := beam.ParDo(scope, NewMarshalFn(elemType), col)
	textio.Write(scope, filename, output)
}
