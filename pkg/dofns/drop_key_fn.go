package dofns

import (
	"context"
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
)

func init() {
	beam.RegisterType(reflect.TypeOf((*DropKeyFn)(nil)))
}

type DropKeyFn struct {
	XType beam.EncodedType
	YType beam.EncodedType
}

func (fn *DropKeyFn) ProcessElement(_ context.Context, _ beam.X, value beam.Y, emit func(beam.Y)) {
	emit(value)
}
