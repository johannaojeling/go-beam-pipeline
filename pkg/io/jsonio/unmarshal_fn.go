package jsonio

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
)

func init() {
	beam.RegisterType(reflect.TypeOf((*UnMarshalFn)(nil)))
}

type UnMarshalFn struct {
	Type beam.EncodedType
}

func NewUnMarshalFn(elemType reflect.Type) *UnMarshalFn {
	return &UnMarshalFn{Type: beam.EncodedType{T: elemType}}
}

func (fn *UnMarshalFn) ProcessElement(
	_ context.Context,
	elem []byte,
	emit func(beam.X),
) error {
	out := reflect.New(fn.Type.T).Interface()
	err := json.Unmarshal(elem, out)
	if err != nil {
		return fmt.Errorf("error unmarshaling json: %v", err)
	}

	newElem := reflect.ValueOf(out).Elem().Interface()
	emit(newElem)
	return nil
}
