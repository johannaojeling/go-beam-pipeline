package mongodbio

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func init() {
	register.DoFn3x1[context.Context, []byte, func(beam.X), error](&readFn{})
	register.Emitter1[beam.X]()
}

type ReadConfig struct {
	URL        string
	Database   string
	Collection string
	BatchSize  int
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
	mongoDBFn
	BatchSize int
	Filter    string
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
		mongoDBFn: mongoDBFn{
			URL:        cfg.URL,
			Database:   cfg.Database,
			Collection: cfg.Collection,
			Type:       beam.EncodedType{T: elemType},
		},
		BatchSize: cfg.BatchSize,
		Filter:    filter,
	}
}

func (fn *readFn) ProcessElement(
	ctx context.Context,
	_ []byte,
	emit func(beam.X),
) error {
	var filter any
	if err := json.Unmarshal([]byte(fn.Filter), &filter); err != nil {
		return fmt.Errorf("error unmarshaling filter to json: %w", err)
	}

	var findOptions []*options.FindOptions
	if fn.BatchSize > 0 {
		findOptions = append(findOptions, options.Find().SetBatchSize(int32(fn.BatchSize)))
	}

	cursor, err := fn.coll.Find(ctx, filter, findOptions...)
	if err != nil {
		return fmt.Errorf("error finding documents: %w", err)
	}
	defer cursor.Close(ctx)

	for cursor.Next(ctx) {
		out := reflect.New(fn.Type.T).Interface()
		if err := cursor.Decode(out); err != nil {
			return fmt.Errorf("error decoding document: %w", err)
		}

		newElem := reflect.ValueOf(out).Elem().Interface()
		emit(newElem)
	}

	return nil
}
