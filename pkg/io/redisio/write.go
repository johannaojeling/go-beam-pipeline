package redisio

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/go-redis/redis/v8"

	"github.com/johannaojeling/go-beam-pipeline/pkg/dofns"
)

const defaultWriteBatchSize = 1000

func init() {
	beam.RegisterType(reflect.TypeOf((*writeFn)(nil)))
}

type WriteConfig struct {
	URL        string
	Expiration time.Duration
	BatchSize  int
	KeyField   string
}

type WriteKVConfig struct {
	URL        string
	Expiration time.Duration
	BatchSize  int
}

func Write(
	scope beam.Scope,
	cfg WriteConfig,
	col beam.PCollection,
) {
	scope = scope.Scope("redisio.Write")
	elemType := col.Type().Type()
	keyed := beam.ParDo(
		scope,
		dofns.NewExtractKeyFn(cfg.KeyField, elemType),
		col,
	)
	kvCfg := WriteKVConfig{
		URL:        cfg.URL,
		Expiration: cfg.Expiration,
		BatchSize:  cfg.BatchSize,
	}
	WriteKV(scope, kvCfg, keyed)
}

func WriteKV(
	scope beam.Scope,
	cfg WriteKVConfig,
	col beam.PCollection,
) {
	scope = scope.Scope("redisio.WriteKV")
	elemType := col.Type().Type()
	beam.ParDo(scope, newWriteFn(cfg, elemType), col)
}

type writeFn struct {
	redisFn
	Expiration time.Duration
	BatchSize  int
	Type       beam.EncodedType
	pipeline   *redis.Pipeliner
	batchCount int
}

func newWriteFn(
	cfg WriteKVConfig,
	elemType reflect.Type,
) *writeFn {
	batchSize := cfg.BatchSize
	if batchSize <= 0 {
		batchSize = defaultWriteBatchSize
	}

	return &writeFn{
		redisFn: redisFn{
			URL: cfg.URL,
		},
		Expiration: cfg.Expiration,
		BatchSize:  batchSize,
		Type:       beam.EncodedType{T: elemType},
	}
}

func (fn *writeFn) StartBundle(_ context.Context, _ func(string)) error {
	pipe := fn.client.Pipeline()
	fn.pipeline = &pipe
	fn.batchCount = 0
	return nil
}

func (fn *writeFn) ProcessElement(
	ctx context.Context,
	key string,
	value beam.X,
	emit func(string),
) error {
	pipeline := fn.pipeline
	pipe := *pipeline
	pipe.Set(ctx, key, value, fn.Expiration)
	fn.batchCount++

	if fn.batchCount >= fn.BatchSize {
		err := fn.flush(ctx)
		if err != nil {
			return err
		}
		pipe = fn.client.Pipeline()
		fn.pipeline = &pipe
	}

	emit(key)
	return nil
}

func (fn *writeFn) FinishBundle(ctx context.Context, _ func(string)) error {
	if fn.batchCount > 0 {
		err := fn.flush(ctx)
		if err != nil {
			return fmt.Errorf("error flushing: %v", err)
		}
	}
	return nil
}

func (fn *writeFn) flush(ctx context.Context) error {
	pipeline := fn.pipeline
	pipe := *pipeline
	_, err := pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("error executing commands: %v", err)
	}
	fn.pipeline = nil
	fn.batchCount = 0
	return nil
}
