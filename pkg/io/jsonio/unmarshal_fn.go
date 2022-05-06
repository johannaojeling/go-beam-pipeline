package jsonio

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
)

func init() {
	beam.RegisterType(reflect.TypeOf((*UnMarshalJsonFn)(nil)))
}

type UnMarshalJsonFn struct {
	Type beam.EncodedType
}

func (fn *UnMarshalJsonFn) ProcessElement(
	_ context.Context,
	elem string,
	emit func(beam.X),
) error {
	out := reflect.New(fn.Type.T).Interface()
	err := json.Unmarshal([]byte(elem), out)
	if err != nil {
		return fmt.Errorf("error unmarshaling json: %v", err)
	}

	newElem := reflect.ValueOf(out).Elem().Interface()
	emit(newElem)
	return nil
}
