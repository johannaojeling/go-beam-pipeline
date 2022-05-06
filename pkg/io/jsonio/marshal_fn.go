package jsonio

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
)

func init() {
	beam.RegisterType(reflect.TypeOf((*MarshalJsonFn)(nil)))
}

type MarshalJsonFn struct {
	Type beam.EncodedType
}

func (fn *MarshalJsonFn) ProcessElement(
	_ context.Context,
	elem beam.X,
	emit func(string),
) error {
	jsonBytes, err := json.Marshal(elem)
	if err != nil {
		return fmt.Errorf("error marshaling json: %v", err)
	}

	emit(string(jsonBytes))
	return nil
}
