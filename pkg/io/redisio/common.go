package redisio

import (
	"context"
	"fmt"
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/go-redis/redis/v8"
)

func init() {
	beam.RegisterType(reflect.TypeOf((*redisFn)(nil)))
}

type redisFn struct {
	URL    string
	client *redis.Client
}

func (fn *redisFn) Setup() error {
	ctx := context.Background()
	client, err := newClient(ctx, fn.URL)
	if err != nil {
		return fmt.Errorf("error initializing Redis client: %v", err)
	}
	fn.client = client
	return nil
}

func (fn *redisFn) Teardown() error {
	err := fn.client.Close()
	if err != nil {
		return fmt.Errorf("error closing Redis client: %v", err)
	}
	return nil
}

func newClient(ctx context.Context, url string) (*redis.Client, error) {
	opt, err := redis.ParseURL(url)
	if err != nil {
		return nil, fmt.Errorf("error parsing URL: %v", err)
	}

	client := redis.NewClient(opt)
	_, err = client.Ping(ctx).Result()
	if err != nil {
		return nil, fmt.Errorf("error pinging Redis: %v", err)
	}
	return client, nil
}
