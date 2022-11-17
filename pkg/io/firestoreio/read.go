package firestoreio

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
	"google.golang.org/api/iterator"
)

func init() {
	register.DoFn3x1[context.Context, []byte, func(beam.X), error](&readFn{})
	register.Emitter1[beam.X]()
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
		if errors.Is(err, iterator.Done) {
			break
		}

		if err != nil {
			return fmt.Errorf("error iterating: %w", err)
		}

		out := reflect.New(fn.Type.T).Interface()
		if err := docSnap.DataTo(out); err != nil {
			return fmt.Errorf("error parsing document: %w", err)
		}

		newElem := reflect.ValueOf(out).Elem().Interface()
		emit(newElem)
	}

	return nil
}
