package sink

import (
	"fmt"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"

	"github.com/johannaojeling/go-beam-pipeline/pkg/pipeline/sink/bigquery"
	"github.com/johannaojeling/go-beam-pipeline/pkg/pipeline/sink/elasticsearch"
	"github.com/johannaojeling/go-beam-pipeline/pkg/pipeline/sink/file"
	"github.com/johannaojeling/go-beam-pipeline/pkg/pipeline/sink/firestore"
	"github.com/johannaojeling/go-beam-pipeline/pkg/pipeline/sink/redis"
)

type Sink struct {
	Format        Format                      `yaml:"format"`
	BigQuery      bigquery.BigQuery           `yaml:"bigquery"`
	Elasticsearch elasticsearch.Elasticsearch `yaml:"elasticsearch"`
	File          file.File                   `yaml:"file"`
	Firestore     firestore.Firestore         `yaml:"firestore"`
	Redis         redis.Redis                 `yaml:"redis"`
}

func (sink Sink) Write(scope beam.Scope, col beam.PCollection) error {
	scope = scope.Scope("Write to sink")
	switch format := sink.Format; format {
	case BigQuery:
		sink.BigQuery.Write(scope, col)
		return nil
	case Elasticsearch:
		return sink.Elasticsearch.Write(scope, col)
	case File:
		return sink.File.Write(scope, col)
	case Firestore:
		sink.Firestore.Write(scope, col)
		return nil
	case Redis:
		return sink.Redis.Write(scope, col)
	default:
		return fmt.Errorf("sink format %q is not supported", format)
	}
}
