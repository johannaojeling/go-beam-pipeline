package stringio

import (
	"context"
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
)

func init() {
	beam.RegisterType(reflect.TypeOf((*EncodeFn)(nil)))
}

type EncodeFn struct{}

func NewEncodeFn() *EncodeFn {
	return &EncodeFn{}
}

func (fn *EncodeFn) ProcessElement(
	_ context.Context,
	elem string,
	emit func([]byte),
) {
	encoded := []byte(elem)
	emit(encoded)
}
