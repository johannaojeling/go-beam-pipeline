package redis

import (
	"context"
	"fmt"
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"

	"github.com/johannaojeling/go-beam-pipeline/pkg/io/redisio"
	"github.com/johannaojeling/go-beam-pipeline/pkg/utils/gcp"
)

type Redis struct {
	Url         URL      `yaml:"url"`
	KeyPatterns []string `yaml:"key_patterns"`
}

type URL struct {
	Value  string `yaml:"value"`
	Secret string `yaml:"secret"`
}

func (redis Redis) Read(scope beam.Scope, elemType reflect.Type) (beam.PCollection, error) {
	scope = scope.Scope("Read from Redis")

	var urlValue string
	if secret := redis.Url.Secret; secret != "" {
		var err error
		urlValue, err = gcp.ReadSecret(context.Background(), secret)
		if err != nil {
			return beam.PCollection{}, fmt.Errorf("error retrieving URL secret: %v", err)
		}
	} else {
		urlValue = redis.Url.Value
	}

	return redisio.Read(scope, urlValue, redis.KeyPatterns, elemType), nil
}
