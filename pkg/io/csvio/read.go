package csvio

import (
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/textio"
)

func Read(
	scope beam.Scope,
	inputPath string,
	elemType reflect.Type,
) beam.PCollection {
	scope = scope.Scope("Read from csv")
	col := textio.ReadSdf(scope, inputPath)
	return beam.ParDo(
		scope,
		&UnMarshalFn{Type: beam.EncodedType{T: elemType}},
		col,
		beam.TypeDefinition{Var: beam.XType, T: elemType},
	)
}
