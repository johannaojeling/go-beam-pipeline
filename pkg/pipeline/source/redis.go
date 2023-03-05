package source

import (
	"context"
	"fmt"
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/johannaojeling/go-beam-pipeline/pkg/io/redisio"
	"github.com/johannaojeling/go-beam-pipeline/pkg/options"
	"github.com/johannaojeling/go-beam-pipeline/pkg/utils/gcp"
)

func ReadFromRedis(
	ctx context.Context,
	scope beam.Scope,
	opt options.RedisReadOption,
	secretReader *gcp.SecretReader,
	elemType reflect.Type,
) (beam.PCollection, error) {
	scope = scope.Scope("Read from Redis")

	url, err := opt.URL.GetValue(ctx, secretReader)
	if err != nil {
		return beam.PCollection{}, fmt.Errorf("error getting URL value: %w", err)
	}

	cfg := redisio.ReadConfig{
		URL:         url,
		KeyPatterns: opt.KeyPatterns,
		BatchSize:   opt.BatchSize,
	}

	return redisio.Read(scope, cfg, elemType), nil
}
