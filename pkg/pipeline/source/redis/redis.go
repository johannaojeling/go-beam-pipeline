package redis

import (
	"context"
	"fmt"
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"

	"github.com/johannaojeling/go-beam-pipeline/pkg/io/redisio"
	"github.com/johannaojeling/go-beam-pipeline/pkg/utils/creds"
	"github.com/johannaojeling/go-beam-pipeline/pkg/utils/gcp"
)

type Redis struct {
	URL         creds.Credential `yaml:"url"`
	KeyPatterns []string         `yaml:"key_patterns"`
	BatchSize   int              `yaml:"batch_size"`
}

func (redis *Redis) Read(
	ctx context.Context,
	secretReader *gcp.SecretReader,
	scope beam.Scope,
	elemType reflect.Type,
) (beam.PCollection, error) {
	scope = scope.Scope("Read from Redis")

	url, err := redis.URL.GetValue(ctx, secretReader)
	if err != nil {
		return beam.PCollection{}, fmt.Errorf("error getting URL value: %w", err)
	}

	cfg := redisio.ReadConfig{
		URL:         url,
		KeyPatterns: redis.KeyPatterns,
		BatchSize:   redis.BatchSize,
	}

	return redisio.Read(scope, cfg, elemType), nil
}
