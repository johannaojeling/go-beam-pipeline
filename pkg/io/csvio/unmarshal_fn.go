package csvio

import (
	"context"
	"fmt"
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"

	"github.com/johannaojeling/go-beam-pipeline/pkg/io/csvio/csv"
)

func init() {
	beam.RegisterType(reflect.TypeOf((*UnMarshalFn)(nil)))
}

type UnMarshalFn struct {
	Type beam.EncodedType
}

func NewUnMarshalFn(elemType reflect.Type) *UnMarshalFn {
	return &UnMarshalFn{beam.EncodedType{T: elemType}}
}

func (fn *UnMarshalFn) ProcessElement(
	_ context.Context,
	elem string,
	emit func(beam.X),
) error {
	out := reflect.New(fn.Type.T).Interface()
	err := csv.Unmarshal(elem, out)
	if err != nil {
		return fmt.Errorf("error unmarshaling csv: %v", err)
	}

	newElem := reflect.ValueOf(out).Elem().Interface()
	emit(newElem)
	return nil
}
