package firestoreio

import (
	"context"
	"fmt"
	"reflect"

	"cloud.google.com/go/firestore"
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
)

const MaxBatchSize = 500

func init() {
	beam.RegisterType(reflect.TypeOf((*createIdFn)(nil)))
	beam.RegisterType(reflect.TypeOf((*writeFn)(nil)))
}

func Write(
	scope beam.Scope,
	project string,
	collection string,
	batchSize int,
	col beam.PCollection,
) {
	scope = scope.Scope("firestoreio.Write")
	elemType := col.Type().Type()
	keyed := beam.ParDo(
		scope,
		&createIdFn{
			firestoreFn{
				Project:    project,
				Collection: collection,
				Type:       beam.EncodedType{T: elemType},
			},
		},
		col,
	)

	keyedType := keyed.Type().Type()
	beam.ParDo(
		scope,
		&writeFn{
			firestoreFn: firestoreFn{
				Project:    project,
				Collection: collection,
				Type:       beam.EncodedType{T: keyedType},
			},
			BatchSize: batchSize,
		},
		keyed,
	)
}

type createIdFn struct {
	firestoreFn
}

func (fn *createIdFn) ProcessElement(
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

func (fn *writeFn) StartBundle(_ context.Context, _ func(string)) error {
	if fn.BatchSize <= 0 || fn.BatchSize > MaxBatchSize {
		fn.BatchSize = MaxBatchSize
	}
	fn.batch = fn.client.Batch()
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
		err := fn.flush(ctx)
		if err != nil {
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
	_, err := fn.batch.Commit(ctx)
	if err != nil {
		return fmt.Errorf("failed to commit batch: %v", err)
	}
	fn.batch = nil
	fn.batchCount = 0
	return nil
}
