package pipeline

import (
	"fmt"
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"

	"github.com/johannaojeling/go-beam-pipeline/pkg/pipeline/sink"
	"github.com/johannaojeling/go-beam-pipeline/pkg/pipeline/source"
)

type Options struct {
	Source source.Source `yaml:"source"`
	Sink   sink.Sink     `yaml:"sink"`
}

func (options Options) Construct(
	elemType reflect.Type,
) (*beam.Pipeline, error) {
	pipeline, scope := beam.NewPipelineWithRoot()

	col, err := options.Source.Read(scope, elemType)
	if err != nil {
		return nil, fmt.Errorf("error creating source transform: %v", err)
	}

	err = options.Sink.Write(scope, col)
	if err != nil {
		return nil, fmt.Errorf("error creating sink transform: %v", err)
	}

	return pipeline, nil
}
