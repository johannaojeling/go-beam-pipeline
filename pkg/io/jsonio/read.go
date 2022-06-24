package jsonio

import (
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/textio"

	"github.com/johannaojeling/go-beam-pipeline/pkg/io/stringio"
)

func Read(scope beam.Scope, path string, elemType reflect.Type) beam.PCollection {
	scope = scope.Scope("Read from json")
	col := textio.ReadSdf(scope, path)
	encoded := beam.ParDo(scope, stringio.NewEncodeFn(), col)
	return beam.ParDo(
		scope,
		NewUnMarshalFn(elemType),
		encoded,
		beam.TypeDefinition{Var: beam.XType, T: elemType},
	)
}
