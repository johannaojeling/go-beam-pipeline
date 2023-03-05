package sink

import (
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/johannaojeling/go-beam-pipeline/pkg/io/firestoreio"
	"github.com/johannaojeling/go-beam-pipeline/pkg/options"
)

func WriteToFirestore(scope beam.Scope, opt options.FirestoreWriteOption, col beam.PCollection) {
	scope = scope.Scope("Write to Firestore")
	cfg := firestoreio.WriteConfig{
		Project:    opt.Project,
		Collection: opt.Collection,
		BatchSize:  opt.BatchSize,
	}
	firestoreio.Write(scope, cfg, col)
}
