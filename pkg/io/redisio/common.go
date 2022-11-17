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
		return fmt.Errorf("error initializing Redis client: %w", err)
	}

	fn.client = client

	return nil
}

func (fn *redisFn) Teardown() error {
	if err := fn.client.Close(); err != nil {
		return fmt.Errorf("error closing Redis client: %w", err)
	}

	return nil
}

func newClient(ctx context.Context, url string) (*redis.Client, error) {
	opt, err := redis.ParseURL(url)
	if err != nil {
		return nil, fmt.Errorf("error parsing URL: %w", err)
	}

	client := redis.NewClient(opt)
	if _, err := client.Ping(ctx).Result(); err != nil {
		return nil, fmt.Errorf("error pinging Redis: %w", err)
	}

	return client, nil
}
