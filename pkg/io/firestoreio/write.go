package firestoreio

import (
	"context"
	"fmt"
	"reflect"

	"cloud.google.com/go/firestore"
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
)

const defaultWriteBatchSize = 500

func init() {
	register.DoFn3x1[context.Context, beam.X, func(string, beam.X), error](&createIDFn{})
	register.Emitter2[string, beam.X]()
	register.DoFn4x1[context.Context, string, beam.X, func(string), error](&writeFn{})
	register.Emitter1[string]()
}

type WriteConfig struct {
	Project    string
	Collection string
	BatchSize  int
}

func Write(
	scope beam.Scope,
	cfg WriteConfig,
	col beam.PCollection,
) {
	scope = scope.Scope("firestoreio.Write")
	elemType := col.Type().Type()
	keyed := beam.ParDo(
		scope,
		newCreateIDFn(cfg.Project, cfg.Collection, elemType),
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

type createIDFn struct {
	firestoreFn
}

func newCreateIDFn(project string, collection string, elemType reflect.Type) *createIDFn {
	return &createIDFn{
		firestoreFn{
			Project:    project,
			Collection: collection,
			Type:       beam.EncodedType{T: elemType},
		},
	}
}

func (fn *createIDFn) ProcessElement(
	_ context.Context,
	elem beam.X,
	emit func(string, beam.X),
) error {
	docRef := fn.collectionRef.NewDoc()
	emit(docRef.ID, elem)

	return nil
}

type writeFn struct {
	firestoreFn
	BatchSize  int
	batch      *firestore.WriteBatch
	batchCount int
}

func newWriteFn(cfg WriteConfig, elemType reflect.Type) *writeFn {
	batchSize := cfg.BatchSize
	if batchSize <= 0 {
		batchSize = defaultWriteBatchSize
	}

	return &writeFn{
		firestoreFn: firestoreFn{
			Project:    cfg.Project,
			Collection: cfg.Collection,
			Type:       beam.EncodedType{T: elemType},
		},
		BatchSize: batchSize,
	}
}

func (fn *writeFn) StartBundle(_ context.Context, _ func(string)) error {
	fn.batch = fn.client.Batch()
	fn.batchCount = 0

	return nil
}

func (fn *writeFn) ProcessElement(
	ctx context.Context,
	id string,
	elem beam.X,
	emit func(string),
) error {
	docRef := fn.collectionRef.Doc(id)
	fn.batch.Create(docRef, &elem)
	fn.batchCount++

	if fn.batchCount >= fn.BatchSize {
		if err := fn.flush(ctx); err != nil {
			return err
		}

		fn.batch = fn.client.Batch()
	}

	emit(id)

	return nil
}

func (fn *writeFn) FinishBundle(ctx context.Context, _ func(string)) error {
	if fn.batchCount > 0 {
		return fn.flush(ctx)
	}

	return nil
}

func (fn *writeFn) flush(ctx context.Context) error {
	if _, err := fn.batch.Commit(ctx); err != nil {
		return fmt.Errorf("error committing batch: %w", err)
	}

	fn.batch = nil
	fn.batchCount = 0

	return nil
}
