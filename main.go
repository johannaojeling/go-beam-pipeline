package main

import (
	"context"
	"flag"
	"log"
	"os"
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"

	"github.com/johannaojeling/go-beam-pipeline/pkg/pipeline"
	"github.com/johannaojeling/go-beam-pipeline/pkg/utils"
)

var (
	configPath = flag.String("configPath", "", "Path to configuration file")
	project    = os.Getenv("PROJECT")
	bucket     = os.Getenv("BUCKET")
)

type User struct {
	Id        int    `json:"id"         bigquery:"id"         firestore:"id"`
	FirstName string `json:"first_name" bigquery:"first_name" firestore:"first_name"`
	LastName  string `json:"last_name"  bigquery:"last_name"  firestore:"last_name"`
	Email     string `json:"email"      bigquery:"email"      firestore:"email"`
}

func init() {
	beam.RegisterType(reflect.TypeOf((*User)(nil)))
}

func main() {
	flag.Parse()
	beam.Init()

	ctx := context.Background()
	content, err := utils.ReadFile(ctx, *configPath)
	if err != nil {
		log.Fatalf("error reading config file: %v", err)
	}

	fields := struct {
		Project string
		Bucket  string
	}{
		Project: project,
		Bucket:  bucket,
	}

	var options pipeline.Options
	err = utils.ParseConfig(string(content), fields, &options)
	if err != nil {
		log.Fatalf("error parsing config to Options: %v", err)
	}

	elemType := reflect.TypeOf(User{})
	beamPipeline, err := options.Construct(elemType)
	if err != nil {
		log.Fatalf("error constructing pipeline: %v", err)
	}

	err = beamx.Run(ctx, beamPipeline)
	if err != nil {
		log.Fatalf("error executing job: %v", err)
	}
}
