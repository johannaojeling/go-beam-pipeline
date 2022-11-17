package csvio

import (
	"context"
	"fmt"
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"

	"github.com/johannaojeling/go-beam-pipeline/pkg/io/csvio/csv"
)

func init() {
	register.DoFn3x1[context.Context, beam.X, func(string), error](&MarshalFn{})
	register.Emitter1[string]()
}

type MarshalFn struct {
	Type beam.EncodedType
}

func NewMarshalFn(elemType reflect.Type) *MarshalFn {
	return &MarshalFn{beam.EncodedType{T: elemType}}
}

func (fn *MarshalFn) ProcessElement(
	_ context.Context,
	elem beam.X,
	emit func(string),
) error {
	csvLine, err := csv.Marshal(elem)
	if err != nil {
		return fmt.Errorf("error marshaling csv: %w", err)
	}

	emit(csvLine)

	return nil
}
