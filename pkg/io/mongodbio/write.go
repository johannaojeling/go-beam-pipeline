package mongodbio

import (
	"context"
	"fmt"
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

const defaultWriteBatchSize = 500

func init() {
	register.DoFn3x1[context.Context, beam.X, func(primitive.ObjectID, beam.X), error](
		&createIdFn{},
	)
	register.Emitter2[primitive.ObjectID, beam.X]()
	register.DoFn4x1[context.Context, primitive.ObjectID, beam.X, func(string), error](&writeFn{})
	register.Emitter1[string]()
}

type WriteConfig struct {
	URL        string
	Database   string
	Collection string
	BatchSize  int
}

func Write(
	scope beam.Scope,
	cfg WriteConfig,
	col beam.PCollection,
) {
	scope = scope.Scope("mongodbio.Write")
	elemType := col.Type().Type()
	keyed := beam.ParDo(
		scope,
		newCreateIdFn(elemType),
		col,
	)

	shuffled := beam.Reshuffle(scope, keyed)
	shuffledType := shuffled.Type().Type()

	beam.ParDo(
		scope,
		newWriteFn(cfg, shuffledType),
		shuffled,
	)
}

type createIdFn struct {
	Type beam.EncodedType
}

func newCreateIdFn(elemType reflect.Type) *createIdFn {
	return &createIdFn{
		Type: beam.EncodedType{T: elemType},
	}
}

func (fn *createIdFn) ProcessElement(
	_ context.Context,
	elem beam.X,
	emit func(primitive.ObjectID, beam.X),
) error {
	id := primitive.NewObjectID()
	emit(id, elem)
	return nil
}

type writeFn struct {
	mongoDbFn
	BatchSize   int
	writeModels []mongo.WriteModel
}

func newWriteFn(cfg WriteConfig, elemType reflect.Type) *writeFn {
	batchSize := cfg.BatchSize
	if batchSize <= 0 {
		batchSize = defaultWriteBatchSize
	}

	return &writeFn{
		mongoDbFn: mongoDbFn{
			URL:        cfg.URL,
			Database:   cfg.Database,
			Collection: cfg.Collection,
			Type:       beam.EncodedType{T: elemType},
		},
		BatchSize: batchSize,
	}
}

func (fn *writeFn) ProcessElement(
	ctx context.Context,
	id primitive.ObjectID,
	elem beam.X,
	emit func(string),
) error {
	filter := bson.M{"_id": id}
	model := mongo.NewReplaceOneModel().
		SetFilter(filter).
		SetReplacement(elem).
		SetUpsert(true)

	fn.writeModels = append(fn.writeModels, model)

	if len(fn.writeModels) >= fn.BatchSize {
		err := fn.flush(ctx)
		if err != nil {
			return err
		}
	}

	emit(id.Hex())
	return nil
}

func (fn *writeFn) FinishBundle(ctx context.Context, _ func(string)) error {
	if len(fn.writeModels) > 0 {
		return fn.flush(ctx)
	}
	return nil
}

func (fn *writeFn) flush(ctx context.Context) error {
	_, err := fn.coll.BulkWrite(ctx, fn.writeModels)
	if err != nil {
		return fmt.Errorf("error bulk writing: %v", err)
	}
	fn.writeModels = []mongo.WriteModel(nil)
	return nil
}