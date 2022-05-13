package stringio

import (
	"context"
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
)

func init() {
	beam.RegisterType(reflect.TypeOf((*DecodeFn)(nil)))
}

type DecodeFn struct{}

func (fn *DecodeFn) ProcessElement(
	_ context.Context,
	elem []byte,
	emit func(string),
) {
	decoded := string(elem)
	emit(decoded)
}
