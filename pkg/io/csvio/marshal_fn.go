package csvio

import (
	"context"
	"fmt"
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"

	"github.com/johannaojeling/go-beam-pipeline/pkg/io/csvio/csv"
)

func init() {
	beam.RegisterType(reflect.TypeOf((*MarshalFn)(nil)))
}

type MarshalFn struct {
	Type beam.EncodedType
}

func (fn *MarshalFn) ProcessElement(
	_ context.Context,
	elem beam.X,
	emit func(string),
) error {
	csvLine, err := csv.Marshal(elem)
	if err != nil {
		return fmt.Errorf("error marshaling csv: %v", err)
	}

	emit(csvLine)
	return nil
}
