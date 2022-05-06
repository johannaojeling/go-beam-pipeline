package jsonio

import (
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/textio"
)

func Write(scope beam.Scope, outputPath string, col beam.PCollection) {
	scope = scope.Scope("Write to json")
	elemType := col.Type().Type()
	output := beam.ParDo(scope, &MarshalJsonFn{Type: beam.EncodedType{T: elemType}}, col)
	textio.Write(scope, outputPath, output)
}
