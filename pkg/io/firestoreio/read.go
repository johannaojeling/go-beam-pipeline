package firestoreio

import (
	"context"
	"fmt"
	"reflect"

	"cloud.google.com/go/firestore"
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"google.golang.org/api/iterator"
)

func init() {
	beam.RegisterType(reflect.TypeOf((*readFn)(nil)))
}

func Read(scope beam.Scope, project, collection string, elemType reflect.Type) beam.PCollection {
	impulse := beam.Impulse(scope)
	return beam.ParDo(
		scope,
		&readFn{Project: project, Collection: collection, Type: beam.EncodedType{T: elemType}},
		impulse,
		beam.TypeDefinition{Var: beam.XType, T: elemType},
	)
}

type readFn struct {
	Project       string
	Collection    string
	Type          beam.EncodedType
	client        *firestore.Client
	collectionRef *firestore.CollectionRef
}

func (fn *readFn) Setup() error {
	client, err := firestore.NewClient(context.Background(), fn.Project)
	if err != nil {
		return fmt.Errorf("error initializing Firestore client: %v", err)
	}
	fn.client = client
	fn.collectionRef = client.Collection(fn.Collection)
	return nil
}

func (fn *readFn) ProcessElement(
	ctx context.Context,
	_ []byte,
	emit func(beam.X),
) error {
	iter := fn.collectionRef.Documents(ctx)
	defer iter.Stop()

	for {
		docSnap, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return fmt.Errorf("error iterating: %v", err)
		}

		out := reflect.New(fn.Type.T).Interface()
		err = docSnap.DataTo(out)
		if err != nil {
			return fmt.Errorf("error parsing document: %v", err)
		}

		newElem := reflect.ValueOf(out).Elem().Interface()
		emit(newElem)
	}
	return nil
}

func (fn *readFn) Teardown() error {
	err := fn.client.Close()
	if err != nil {
		return fmt.Errorf("error closing Firestore client: %v", err)
	}
	return nil
}
