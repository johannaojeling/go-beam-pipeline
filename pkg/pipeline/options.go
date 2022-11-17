package pipeline

import (
	"context"
	"fmt"
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"

	"github.com/johannaojeling/go-beam-pipeline/pkg/pipeline/sink"
	"github.com/johannaojeling/go-beam-pipeline/pkg/pipeline/source"
	"github.com/johannaojeling/go-beam-pipeline/pkg/utils/gcp"
)

type Options struct {
	Source *source.Source `yaml:"source"`
	Sink   *sink.Sink     `yaml:"sink"`
}

func (options *Options) Construct(
	ctx context.Context,
	secretReader *gcp.SecretReader,
	elemType reflect.Type,
) (*beam.Pipeline, error) {
	pipeline, scope := beam.NewPipelineWithRoot()

	col, err := options.Source.Read(ctx, secretReader, scope, elemType)
	if err != nil {
		return nil, fmt.Errorf("error creating source transform: %w", err)
	}

	if err := options.Sink.Write(ctx, secretReader, scope, col); err != nil {
		return nil, fmt.Errorf("error creating sink transform: %w", err)
	}

	return pipeline, nil
}
