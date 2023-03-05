package pipeline

import (
	"context"
	"fmt"
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/johannaojeling/go-beam-pipeline/pkg/options"
	"github.com/johannaojeling/go-beam-pipeline/pkg/pipeline/sink"
	"github.com/johannaojeling/go-beam-pipeline/pkg/pipeline/source"
	"github.com/johannaojeling/go-beam-pipeline/pkg/utils/gcp"
)

func Construct(
	ctx context.Context,
	opt options.PipelineOption,
	secretReader *gcp.SecretReader,
	elemType reflect.Type,
) (*beam.Pipeline, error) {
	pipeline, scope := beam.NewPipelineWithRoot()

	col, err := source.Read(ctx, scope, opt.Source, secretReader, elemType)
	if err != nil {
		return nil, fmt.Errorf("error constructing source transform: %w", err)
	}

	if err := sink.Write(ctx, scope, opt.Sink, secretReader, col); err != nil {
		return nil, fmt.Errorf("error constructing sink transform: %w", err)
	}

	return pipeline, nil
}
