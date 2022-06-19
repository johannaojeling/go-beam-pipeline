package redis

import (
	"fmt"
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"

	"github.com/johannaojeling/go-beam-pipeline/pkg/io/redisio"
	"github.com/johannaojeling/go-beam-pipeline/pkg/utils/creds"
)

type Redis struct {
	URL         creds.Credential `yaml:"url"`
	KeyPatterns []string         `yaml:"key_patterns"`
}

func (redis Redis) Read(scope beam.Scope, elemType reflect.Type) (beam.PCollection, error) {
	scope = scope.Scope("Read from Redis")

	url, err := redis.URL.GetValue()
	if err != nil {
		return beam.PCollection{}, fmt.Errorf("failed to get URL value: %v", err)
	}

	return redisio.Read(scope, url, redis.KeyPatterns, elemType), nil
}
