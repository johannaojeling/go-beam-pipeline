package mongodbio

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
)

func init() {
	register.DoFn3x1[context.Context, []byte, func(beam.X), error](&readFn{})
	register.Emitter1[beam.X]()
}

type ReadConfig struct {
	URL        string
	Database   string
	Collection string
	Filter     string
}

func Read(
	scope beam.Scope,
	cfg ReadConfig,
	elemType reflect.Type,
) beam.PCollection {
	scope = scope.Scope("mongodbio.Read")
	impulse := beam.Impulse(scope)
	return beam.ParDo(
		scope,
		newReadFn(cfg, elemType),
		impulse,
		beam.TypeDefinition{Var: beam.XType, T: elemType},
	)
}

type readFn struct {
	Filter string
	mongoDbFn
}

func newReadFn(
	cfg ReadConfig,
	elemType reflect.Type,
) *readFn {
	filter := cfg.Filter
	if filter == "" {
		filter = "{}"
	}

	return &readFn{
		Filter: filter,
		mongoDbFn: mongoDbFn{
			URL:        cfg.URL,
			Database:   cfg.Database,
			Collection: cfg.Collection,
			Type:       beam.EncodedType{T: elemType},
		},
	}
}

func (fn *readFn) ProcessElement(
	ctx context.Context,
	_ []byte,
	emit func(beam.X),
) error {
	var filter interface{}
	err := json.Unmarshal([]byte(fn.Filter), &filter)
	if err != nil {
		return fmt.Errorf("error unmarshaling filter to json: %v", err)
	}

	cursor, err := fn.coll.Find(ctx, filter)
	if err != nil {
		return fmt.Errorf("error finding documents: %v", err)
	}
	defer cursor.Close(ctx)

	for cursor.Next(ctx) {
		out := reflect.New(fn.Type.T).Interface()
		err := cursor.Decode(out)
		if err != nil {
			return fmt.Errorf("error decoding document: %v", err)
		}

		newElem := reflect.ValueOf(out).Elem().Interface()
		emit(newElem)
	}
	return nil
}
