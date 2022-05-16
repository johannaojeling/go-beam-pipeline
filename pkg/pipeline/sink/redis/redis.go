package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"

	"github.com/johannaojeling/go-beam-pipeline/pkg/io/redisio"
	"github.com/johannaojeling/go-beam-pipeline/pkg/utils/gcp"
)

type Redis struct {
	URL        URL           `yaml:"url"`
	Expiration time.Duration `yaml:"expiration"`
	BatchSize  int           `yaml:"batch_size"`
	KeyField   string        `yaml:"key_field"`
}

type URL struct {
	Value  string `yaml:"value"`
	Secret string `yaml:"secret"`
}

func (redis Redis) Write(scope beam.Scope, col beam.PCollection) error {
	scope = scope.Scope("Write to Redis")

	var urlValue string
	if secret := redis.URL.Secret; secret != "" {
		var err error
		urlValue, err = gcp.ReadSecret(context.Background(), secret)
		if err != nil {
			return fmt.Errorf("error retrieving DSN secret: %v", err)
		}
	} else {
		urlValue = redis.URL.Value
	}

	redisio.Write(scope, urlValue, redis.Expiration, redis.BatchSize, redis.KeyField, col)
	return nil
}
