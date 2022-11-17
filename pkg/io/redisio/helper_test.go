package redisio

import (
	"context"
	"fmt"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis/v8"
)

func NewRedis(t *testing.T) *miniredis.Miniredis {
	t.Helper()

	redis, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %s", err)
	}

	t.Cleanup(redis.Close)

	return redis
}

func NewClient(ctx context.Context, t *testing.T, url string) *redis.Client {
	t.Helper()

	opt, err := redis.ParseURL(url)
	if err != nil {
		t.Fatalf("error parsing URL: %v", err)
	}

	client := redis.NewClient(opt)
	if _, err = client.Ping(ctx).Result(); err != nil {
		t.Fatalf("error pinging Redis: %v", err)
	}

	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Fatalf("error closing client: %v", err)
		}
	})

	return client
}

func GetEntries(
	ctx context.Context,
	t *testing.T,
	client *redis.Client,
	prefix string,
) map[string]string {
	t.Helper()

	keys, err := getKeys(ctx, client, prefix)
	if err != nil {
		t.Fatalf("error getting keys: %v", err)
	}

	values, err := getValues(ctx, client, keys)
	if err != nil {
		t.Fatalf("error getting values: %v", err)
	}

	entries := make(map[string]string, len(keys))
	for i := 0; i < len(keys); i++ {
		entries[keys[i]] = values[i]
	}

	return entries
}

func SetEntries(
	ctx context.Context,
	t *testing.T,
	client *redis.Client,
	entries map[string]string,
) {
	t.Helper()

	size := len(entries) * 2
	args := make([]any, 0, size)

	for key, value := range entries {
		args = append(args, key)
		args = append(args, value)
	}

	if err := client.MSet(ctx, args...).Err(); err != nil {
		t.Fatalf("error executing MSET: %v", err)
	}
}

func getKeys(ctx context.Context, client *redis.Client, prefix string) ([]string, error) {
	keys := make([]string, 0)
	iter := client.Scan(ctx, 0, prefix, 0).Iterator()

	for iter.Next(ctx) {
		key := iter.Val()
		keys = append(keys, key)
	}

	if err := iter.Err(); err != nil {
		return nil, fmt.Errorf("error iterating: %w", err)
	}

	return keys, nil
}

func getValues(ctx context.Context, client *redis.Client, keys []string) ([]string, error) {
	values := make([]string, 0)

	results, err := client.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, fmt.Errorf("error executing MGET: %w", err)
	}

	for _, result := range results {
		value, ok := result.(string)
		if !ok {
			return nil, fmt.Errorf("unexpected value type: %T", result)
		}

		values = append(values, value)
	}

	return values, nil
}
