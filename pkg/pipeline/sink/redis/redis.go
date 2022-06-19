package redis

import (
	"fmt"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"

	"github.com/johannaojeling/go-beam-pipeline/pkg/io/redisio"
	"github.com/johannaojeling/go-beam-pipeline/pkg/utils/creds"
)

type Redis struct {
	URL        creds.Credential `yaml:"url"`
	Expiration time.Duration    `yaml:"expiration"`
	BatchSize  int              `yaml:"batch_size"`
	KeyField   string           `yaml:"key_field"`
}

func (redis Redis) Write(scope beam.Scope, col beam.PCollection) error {
	scope = scope.Scope("Write to Redis")

	url, err := redis.URL.GetValue()
	if err != nil {
		return fmt.Errorf("failed to get URL value: %v", err)
	}

	redisio.Write(scope, url, redis.Expiration, redis.BatchSize, redis.KeyField, col)
	return nil
}
