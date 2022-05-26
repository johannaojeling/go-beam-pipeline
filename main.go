package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/options/gcpopts"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"

	"github.com/johannaojeling/go-beam-pipeline/pkg/pipeline"
	"github.com/johannaojeling/go-beam-pipeline/pkg/utils/config"
	"github.com/johannaojeling/go-beam-pipeline/pkg/utils/file"
)

var (
	configPath = flag.String("configPath", "", "Path to configuration file")
	bucket     = flag.String("bucket", "", "Bucket for data storage")
)

type Event struct {
	Timestamp int64  `json:"timestamp"  bigquery:"timestamp"  firestore:"timestamp"  parquet:"name=timestamp, type=INT64, convertedtype=TIMESTAMP_MILLIS"`
	EventType int32  `json:"event_type" bigquery:"event_type" firestore:"event_type" parquet:"name=event_type, type=INT32"`
	EventId   string `json:"event_id"   bigquery:"event_id"   firestore:"event_id"   parquet:"name=event_id, type=BYTE_ARRAY, convertedtype=UTF8"`
	UserId    string `json:"user_id"    bigquery:"user_id"    firestore:"user_id"    parquet:"name=user_id, type=BYTE_ARRAY, convertedtype=UTF8"`
}

func (event Event) MarshalBinary() ([]byte, error) {
	return json.Marshal(event)
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

	var options pipeline.Options
	err = config.ParseConfig(string(content), fields, &options)
	if err != nil {
		log.Fatalf("error parsing config to Options: %v", err)
	}

	elemType := reflect.TypeOf(Event{})
	beamPipeline, err := options.Construct(elemType)
	if err != nil {
		log.Fatalf("error constructing pipeline: %v", err)
	}

	err = beamx.Run(ctx, beamPipeline)
	if err != nil {
		log.Fatalf("error executing job: %v", err)
	}
}
