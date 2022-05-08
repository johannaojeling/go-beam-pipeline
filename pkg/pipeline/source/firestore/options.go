package firestore

import (
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"

	"github.com/johannaojeling/go-beam-pipeline/pkg/io/firestoreio"
)

type Firestore struct {
	ProjectId  string `yaml:"project_id"`
	Collection string `yaml:"collection"`
}

func (firestore Firestore) Read(
	scope beam.Scope,
	elemType reflect.Type) beam.PCollection {
	scope = scope.Scope("Read from Firestore")
	return firestoreio.Read(scope, firestore.ProjectId, firestore.Collection, elemType)
}
