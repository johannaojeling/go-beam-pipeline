package jsonio

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
)

func init() {
	register.DoFn3x1[context.Context, beam.X, func([]byte), error](&MarshalFn{})
	register.Emitter1[[]byte]()
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
	emit func([]byte),
) error {
	jsonBytes, err := json.Marshal(elem)
	if err != nil {
		return fmt.Errorf("error marshaling json: %v", err)
	}

	emit(jsonBytes)
	return nil
}
