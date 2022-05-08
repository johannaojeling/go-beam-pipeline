package sink

import (
	"fmt"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"

	"github.com/johannaojeling/go-beam-pipeline/pkg/pipeline/sink/bigquery"
	"github.com/johannaojeling/go-beam-pipeline/pkg/pipeline/sink/file"
	"github.com/johannaojeling/go-beam-pipeline/pkg/pipeline/sink/firestore"
)

type Sink struct {
	Format    Format              `yaml:"format"`
	File      file.File           `yaml:"file"`
	BigQuery  bigquery.BigQuery   `yaml:"bigquery"`
	Firestore firestore.Firestore `yaml:"firestore"`
}

func (sink Sink) Write(scope beam.Scope, col beam.PCollection) error {
	scope = scope.Scope("Write to sink")
	switch format := sink.Format; format {
	case BigQuery:
		sink.BigQuery.Write(scope, col)
		return nil
	case File:
		return sink.File.Write(scope, col)
	case Firestore:
		sink.Firestore.Write(scope, col)
		return nil
	default:
		return fmt.Errorf("sink format %q is not supported", format)
	}
}
