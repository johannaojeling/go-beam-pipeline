package mongodbio

import (
	"context"
	"fmt"
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
	"go.mongodb.org/mongo-driver/bson"
)

func init() {
	register.DoFn3x1[context.Context, []byte, func(beam.X), error](&readFn{})
	register.Emitter1[beam.X]()
}

type ReadConfig struct {
	URL        string
	Database   string
	Collection string
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
	mongoDbFn
}

func newReadFn(
	cfg ReadConfig,
	elemType reflect.Type,
) *readFn {
	return &readFn{
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
	cursor, err := fn.coll.Find(ctx, bson.M{})
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
