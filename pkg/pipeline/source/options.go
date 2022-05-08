package source

import (
	"fmt"
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"

	"github.com/johannaojeling/go-beam-pipeline/pkg/pipeline/source/bigquery"
	"github.com/johannaojeling/go-beam-pipeline/pkg/pipeline/source/file"
	"github.com/johannaojeling/go-beam-pipeline/pkg/pipeline/source/firestore"
)

type Source struct {
	Format    Format              `yaml:"format"`
	File      file.File           `yaml:"file"`
	BigQuery  bigquery.BigQuery   `yaml:"bigquery"`
	Firestore firestore.Firestore `yaml:"firestore"`
}

func (source Source) Read(
	scope beam.Scope,
	elemType reflect.Type,
) (beam.PCollection, error) {
	scope = scope.Scope("Read from source")
	switch format := source.Format; format {
	case BigQuery:
		return source.BigQuery.Read(scope, elemType), nil
	case File:
		return source.File.Read(scope, elemType)
	case Firestore:
		return source.Firestore.Read(scope, elemType), nil
	default:
		return beam.PCollection{}, fmt.Errorf("source format %q is not supported", format)
	}
}
