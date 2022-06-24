package firestoreio

import (
	"context"
	"fmt"
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"google.golang.org/api/iterator"
)

func init() {
	beam.RegisterType(reflect.TypeOf((*readFn)(nil)))
}

type ReadConfig struct {
	Project    string
	Collection string
}

func Read(
	scope beam.Scope,
	cfg ReadConfig,
	elemType reflect.Type,
) beam.PCollection {
	scope = scope.Scope("firestoreio.Read")
	impulse := beam.Impulse(scope)
	return beam.ParDo(
		scope,
		newReadFn(cfg, elemType),
		impulse,
		beam.TypeDefinition{Var: beam.XType, T: elemType},
	)
}

type readFn struct {
	firestoreFn
}

func newReadFn(
	cfg ReadConfig,
	elemType reflect.Type,
) *readFn {
	return &readFn{
		firestoreFn{
			Project:    cfg.Project,
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
