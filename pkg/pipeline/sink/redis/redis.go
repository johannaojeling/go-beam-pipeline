package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"

	"github.com/johannaojeling/go-beam-pipeline/pkg/io/redisio"
	"github.com/johannaojeling/go-beam-pipeline/pkg/utils/creds"
	"github.com/johannaojeling/go-beam-pipeline/pkg/utils/gcp"
)

type Redis struct {
	URL        creds.Credential `yaml:"url"`
	Expiration time.Duration    `yaml:"expiration"`
	BatchSize  int              `yaml:"batch_size"`
	KeyField   string           `yaml:"key_field"`
}

func (redis Redis) Write(
	ctx context.Context,
	secretReader *gcp.SecretReader,
	scope beam.Scope,
	col beam.PCollection,
) error {
	scope = scope.Scope("Write to Redis")

	url, err := redis.URL.GetValue(ctx, secretReader)
	if err != nil {
		return fmt.Errorf("error getting URL value: %v", err)
	}

	cfg := redisio.WriteConfig{
		URL:        url,
		Expiration: redis.Expiration,
		BatchSize:  redis.BatchSize,
		KeyField:   redis.KeyField,
	}
	redisio.Write(scope, cfg, col)
	return nil
}
