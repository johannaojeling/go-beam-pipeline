package sink

import (
	"context"
	"fmt"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/johannaojeling/go-beam-pipeline/pkg/io/redisio"
	"github.com/johannaojeling/go-beam-pipeline/pkg/options"
	"github.com/johannaojeling/go-beam-pipeline/pkg/utils/gcp"
)

func WriteToRedis(
	ctx context.Context,
	scope beam.Scope,
	opt options.RedisWriteOption,
	secretReader *gcp.SecretReader,
	col beam.PCollection,
) error {
	scope = scope.Scope("Write to Redis")

	url, err := opt.URL.GetValue(ctx, secretReader)
	if err != nil {
		return fmt.Errorf("error getting URL value: %w", err)
	}

	cfg := redisio.WriteConfig{
		URL:        url,
		Expiration: opt.Expiration,
		BatchSize:  opt.BatchSize,
		KeyField:   opt.KeyField,
	}
	redisio.Write(scope, cfg, col)

	return nil
}
