package dofns

import (
	"context"
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
)

func init() {
	register.DoFn4x0[context.Context, beam.X, beam.Y, func(beam.Y)](&DropKeyFn{})
	register.Emitter1[beam.Y]()
}

type DropKeyFn struct {
	XType beam.EncodedType
	YType beam.EncodedType
}

func NewDropKeyFn(keyType reflect.Type, valueType reflect.Type) *DropKeyFn {
	return &DropKeyFn{
		XType: beam.EncodedType{T: keyType},
		YType: beam.EncodedType{T: valueType},
	}
}

func (fn *DropKeyFn) ProcessElement(_ context.Context, _ beam.X, value beam.Y, emit func(beam.Y)) {
	emit(value)
}
