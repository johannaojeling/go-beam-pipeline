package csvio

import (
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/textio"
)

func Read(
	scope beam.Scope,
	path string,
	elemType reflect.Type,
) beam.PCollection {
	scope = scope.Scope("Read from csv")
	col := textio.Read(scope, path)
	return beam.ParDo(
		scope,
		NewUnMarshalFn(elemType),
		col,
		beam.TypeDefinition{Var: beam.XType, T: elemType},
	)
}
