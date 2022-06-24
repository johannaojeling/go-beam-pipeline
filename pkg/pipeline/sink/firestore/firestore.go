package firestore

import (
	"github.com/apache/beam/sdks/v2/go/pkg/beam"

	"github.com/johannaojeling/go-beam-pipeline/pkg/io/firestoreio"
)

type Firestore struct {
	Project    string `yaml:"project"`
	Collection string `yaml:"collection"`
	BatchSize  int    `yaml:"batch_size"`
}

func (firestore Firestore) Write(scope beam.Scope, col beam.PCollection) {
	scope = scope.Scope("Write to Firestore")
	cfg := firestoreio.WriteConfig{
		Project:    firestore.Project,
		Collection: firestore.Collection,
		BatchSize:  firestore.BatchSize,
	}
	firestoreio.Write(scope, cfg, col)
}
