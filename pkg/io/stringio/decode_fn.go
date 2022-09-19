package stringio

import (
	"context"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
)

func init() {
	register.DoFn3x0[context.Context, []byte, func(string)](&DecodeFn{})
	register.Emitter1[string]()
}

type DecodeFn struct{}

func NewDecodeFn() *DecodeFn {
	return &DecodeFn{}
}

func (fn *DecodeFn) ProcessElement(
	_ context.Context,
	elem []byte,
	emit func(string),
) {
	decoded := string(elem)
	emit(decoded)
}
