package firestoreio

import (
	"context"
	"fmt"
	"reflect"

	"cloud.google.com/go/firestore"
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
)

func init() {
	beam.RegisterType(reflect.TypeOf((*firestoreFn)(nil)))
}

type firestoreFn struct {
	Project       string
	Collection    string
	Type          beam.EncodedType
	client        *firestore.Client
	collectionRef *firestore.CollectionRef
}

func (fn *firestoreFn) Setup() error {
	client, err := firestore.NewClient(context.Background(), fn.Project)
	if err != nil {
		return fmt.Errorf("error initializing Firestore client: %v", err)
	}
	fn.client = client
	fn.collectionRef = client.Collection(fn.Collection)
	return nil
}

func (fn *firestoreFn) Teardown() error {
	err := fn.client.Close()
	if err != nil {
		return fmt.Errorf("error closing Firestore client: %v", err)
	}
	return nil
}
