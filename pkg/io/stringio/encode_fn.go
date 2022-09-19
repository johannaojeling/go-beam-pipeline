package stringio

import (
	"context"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
)

func init() {
	register.DoFn3x0[context.Context, string, func([]byte)](&EncodeFn{})
	register.Emitter1[[]byte]()
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
