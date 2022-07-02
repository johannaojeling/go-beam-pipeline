package firestore

import (
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"

	"github.com/johannaojeling/go-beam-pipeline/pkg/io/firestoreio"
)

type Firestore struct {
	Project    string `yaml:"project"`
	Collection string `yaml:"collection"`
}

func (fs *Firestore) Read(
	scope beam.Scope,
	elemType reflect.Type,
) beam.PCollection {
	scope = scope.Scope("Read from Firestore")
	cfg := firestoreio.ReadConfig{
		Project:    fs.Project,
		Collection: fs.Collection,
	}
	return firestoreio.Read(scope, cfg, elemType)
}
