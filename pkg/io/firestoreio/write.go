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
	beam.RegisterType(reflect.TypeOf((*writeFn)(nil)))
}

func Write(scope beam.Scope, project string, collection string, col beam.PCollection) {
	scope = scope.Scope("Write to Firestore")
	elemType := col.Type().Type()
	beam.ParDo(
		scope,
		&writeFn{Project: project, Collection: collection, Type: beam.EncodedType{T: elemType}},
		col,
	)
}

type writeFn struct {
	Project       string
	Collection    string
	Type          beam.EncodedType
	client        *firestore.Client
	collectionRef *firestore.CollectionRef
	batch         *firestore.WriteBatch
	batchCount    int
}

func (fn *writeFn) Setup() error {
	client, err := firestore.NewClient(context.Background(), fn.Project)
	if err != nil {
		return fmt.Errorf("error initializing Firestore client: %v", err)
	}
	fn.client = client
	fn.collectionRef = client.Collection(fn.Collection)
	return nil
}

func (fn *writeFn) StartBundle(_ context.Context, _ func(string)) error {
	fn.batch = fn.client.Batch()
	fn.batchCount = 0
	return nil
}

func (fn *writeFn) ProcessElement(ctx context.Context, elem beam.X, emit func(string)) error {
	docRef := fn.collectionRef.NewDoc()
	fn.batch.Create(docRef, &elem)
	fn.batchCount++

	if fn.batchCount >= MaxBatchSize {
		_, err := fn.batch.Commit(ctx)
		if err != nil {
			return fmt.Errorf("failed to commit batch: %v", err)
		}
		fn.batch = fn.client.Batch()
		fn.batchCount = 0
	}

	emit(docRef.ID)
	return nil
}

func (fn *writeFn) FinishBundle(ctx context.Context, _ func(string)) error {
	if fn.batchCount > 0 {
		_, err := fn.batch.Commit(ctx)
		if err != nil {
			return fmt.Errorf("failed to commit batch: %v", err)
		}
		fn.batch = nil
		fn.batchCount = 0
	}
	return nil
}

func (fn *writeFn) Teardown() error {
	err := fn.client.Close()
	if err != nil {
		return fmt.Errorf("error closing Firestore client: %v", err)
	}
	return nil
}
