package source

import (
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/johannaojeling/go-beam-pipeline/pkg/io/firestoreio"
	"github.com/johannaojeling/go-beam-pipeline/pkg/options"
)

func ReadFromFirestore(
	scope beam.Scope,
	opt options.FirestoreReadOption,
	elemType reflect.Type,
) beam.PCollection {
	scope = scope.Scope("Read from Firestore")
	cfg := firestoreio.ReadConfig{
		Project:    opt.Project,
		Collection: opt.Collection,
	}

	return firestoreio.Read(scope, cfg, elemType)
}
