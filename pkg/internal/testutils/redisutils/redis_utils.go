package redisutils

import (
	"context"
	"fmt"

	"github.com/go-redis/redis/v8"
)

func GetEntries(url string, prefix string) (map[string]string, error) {
	ctx := context.Background()
	client, err := newClient(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("error intializing Redis client: %v", err)
	}
	defer client.Close()

	keys, err := getKeys(ctx, client, prefix)
	if err != nil {
		return nil, fmt.Errorf("error getting keys: %v", err)
	}

	values, err := getValues(ctx, client, keys)
	if err != nil {
		return nil, fmt.Errorf("error getting values: %v", err)
	}

	entries := make(map[string]string, len(keys))
	for i := 0; i < len(keys); i++ {
		entries[keys[i]] = values[i]
	}

	return entries, nil
}

func SetEntries(url string, entries map[string]string) error {
	ctx := context.Background()
	client, err := newClient(ctx, url)
	if err != nil {
		return fmt.Errorf("error intializing Redis client: %v", err)
	}
	defer client.Close()

	size := len(entries) * 2
	args := make([]interface{}, 0, size)
	for key, value := range entries {
		args = append(args, key)
		args = append(args, value)
	}

	err = client.MSet(ctx, args...).Err()
	if err != nil {
		return fmt.Errorf("error executing MSET: %v", err)
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

func getKeys(ctx context.Context, client *redis.Client, prefix string) ([]string, error) {
	keys := make([]string, 0)
	iter := client.Scan(ctx, 0, prefix, 0).Iterator()
	for iter.Next(ctx) {
		key := iter.Val()
		keys = append(keys, key)
	}
	err := iter.Err()
	if err != nil {
		return nil, fmt.Errorf("error iterating: %v", err)
	}
	return keys, nil
}

func getValues(ctx context.Context, client *redis.Client, keys []string) ([]string, error) {
	values := make([]string, 0)
	results, err := client.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, fmt.Errorf("error executing MGET: %v", err)
	}

	for _, result := range results {
		value := result.(string)
		values = append(values, value)
	}

	return values, nil
}
