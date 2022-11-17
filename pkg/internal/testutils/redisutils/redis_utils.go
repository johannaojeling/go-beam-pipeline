package redisutils

import (
	"context"
	"fmt"

	"github.com/go-redis/redis/v8"
)

func NewClient(ctx context.Context, url string) (*redis.Client, error) {
	opt, err := redis.ParseURL(url)
	if err != nil {
		return nil, fmt.Errorf("error parsing URL: %w", err)
	}

	client := redis.NewClient(opt)
	if _, err = client.Ping(ctx).Result(); err != nil {
		return nil, fmt.Errorf("error pinging Redis: %w", err)
	}

	return client, nil
}

func GetEntries(
	ctx context.Context,
	client *redis.Client,
	prefix string,
) (map[string]string, error) {
	keys, err := getKeys(ctx, client, prefix)
	if err != nil {
		return nil, fmt.Errorf("error getting keys: %w", err)
	}

	values, err := getValues(ctx, client, keys)
	if err != nil {
		return nil, fmt.Errorf("error getting values: %w", err)
	}

	entries := make(map[string]string, len(keys))
	for i := 0; i < len(keys); i++ {
		entries[keys[i]] = values[i]
	}

	return entries, nil
}

func SetEntries(ctx context.Context, client *redis.Client, entries map[string]string) error {
	size := len(entries) * 2
	args := make([]any, 0, size)

	for key, value := range entries {
		args = append(args, key)
		args = append(args, value)
	}

	if err := client.MSet(ctx, args...).Err(); err != nil {
		return fmt.Errorf("error executing MSET: %w", err)
	}

	return nil
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
