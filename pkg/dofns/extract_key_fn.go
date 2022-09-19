package dofns

import (
	"context"
	"fmt"
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
)

func init() {
	register.DoFn3x1[context.Context, beam.X, func(string, beam.X), error](&ExtractKeyFn{})
	register.Emitter2[string, beam.X]()
}

type ExtractKeyFn struct {
	KeyField string
	Type     beam.EncodedType
}

func NewExtractKeyFn(keyField string, elemType reflect.Type) *ExtractKeyFn {
	return &ExtractKeyFn{
		KeyField: keyField,
		Type:     beam.EncodedType{T: elemType},
	}
}

func (fn *ExtractKeyFn) ProcessElement(
	_ context.Context,
	elem beam.X,
	emit func(string, beam.X),
) error {
	val := reflect.ValueOf(elem)
	kind := val.Kind()
	if kind != reflect.Struct {
		return fmt.Errorf("element must be a struct but was: %v", kind)
	}

	key, err := extractKey(val, fn.KeyField)
	if err != nil {
		return fmt.Errorf("error extracting key: %v", err)
	}

	emit(key, elem)
	return nil
}

func extractKey(val reflect.Value, keyField string) (string, error) {
	field := val.FieldByName(keyField)
	if field == (reflect.Value{}) {
		return "", fmt.Errorf("element has no field with name %q", keyField)
	}
	return field.String(), nil
}
