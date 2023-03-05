package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/options/gcpopts"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
	_ "github.com/go-sql-driver/mysql"
	"github.com/johannaojeling/go-beam-pipeline/pkg/options"
	"github.com/johannaojeling/go-beam-pipeline/pkg/pipeline"
	"github.com/johannaojeling/go-beam-pipeline/pkg/utils/config"
	"github.com/johannaojeling/go-beam-pipeline/pkg/utils/file"
	"github.com/johannaojeling/go-beam-pipeline/pkg/utils/gcp"
	_ "github.com/lib/pq"
)

var (
	configPath = flag.String("configPath", "", "Path to configuration file")
	bucket     = flag.String("bucket", "", "Bucket for data storage")
)

type Event struct {
	Timestamp int64  `json:"timestamp"  bson:"timestamp"  bigquery:"timestamp"  firestore:"timestamp"  parquet:"name=timestamp, type=INT64, convertedtype=TIMESTAMP_MILLIS"`
	EventType int32  `json:"event_type" bson:"event_type" bigquery:"event_type" firestore:"event_type" parquet:"name=event_type, type=INT32"`
	EventID   string `json:"event_id"   bson:"event_id"   bigquery:"event_id"   firestore:"event_id"   parquet:"name=event_id, type=BYTE_ARRAY, convertedtype=UTF8"`
	UserID    string `json:"user_id"    bson:"user_id"    bigquery:"user_id"    firestore:"user_id"    parquet:"name=user_id, type=BYTE_ARRAY, convertedtype=UTF8"`
}

func (event Event) MarshalBinary() ([]byte, error) {
	data, err := json.Marshal(event)
	if err != nil {
		return nil, fmt.Errorf("error marshaling event: %w", err)
	}

	return data, nil
}

func init() {
	beam.RegisterType(reflect.TypeOf((*Event)(nil)))
}

func main() {
	flag.Parse()
	beam.Init()

	ctx := context.Background()

	content, err := file.ReadFile(ctx, *configPath)
	if err != nil {
		log.Fatalf("error reading config file: %v", err)
	}

	fields := struct {
		Project string
		Bucket  string
	}{
		Project: *gcpopts.Project,
		Bucket:  *bucket,
	}

	var opt options.PipelineOption
	if err := config.ParseConfig(string(content), fields, &opt); err != nil {
		log.Fatalf("Failed to parse config to pipeline options: %v", err)
	}

	secretReader := gcp.NewSecretReader()
	defer secretReader.Close()

	elemType := reflect.TypeOf(Event{})

	beamPipeline, err := pipeline.Construct(ctx, opt, secretReader, elemType)
	if err != nil {
		log.Fatalf("Failed to construct pipeline: %v", err)
	}

	if err = beamx.Run(ctx, beamPipeline); err != nil {
		log.Fatalf("Failed to execute job: %v", err)
	}
}
