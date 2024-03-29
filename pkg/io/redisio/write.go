package redisio

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
	"github.com/go-redis/redis/v8"

	"github.com/johannaojeling/go-beam-pipeline/pkg/dofns"
)

const defaultWriteBatchSize = 1000

func init() {
	register.DoFn4x1[context.Context, string, beam.X, func(string), error](&writeFn{})
	register.Emitter1[string]()
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
	pipeline   redis.Pipeliner
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

func (fn *writeFn) StartBundle(_ context.Context, _ func(string)) {
	fn.pipeline = fn.client.Pipeline()
}

func (fn *writeFn) ProcessElement(
	ctx context.Context,
	key string,
	value beam.X,
	emit func(string),
) error {
	fn.pipeline.Set(ctx, key, value, fn.Expiration)
	fn.batchCount++

	if fn.batchCount >= fn.BatchSize {
		if err := fn.flush(ctx); err != nil {
			return err
		}

		fn.pipeline = fn.client.Pipeline()
	}

	emit(key)

	return nil
}

func (fn *writeFn) FinishBundle(ctx context.Context, _ func(string)) error {
	if fn.batchCount > 0 {
		if err := fn.flush(ctx); err != nil {
			return fmt.Errorf("error flushing: %w", err)
		}
	}

	return nil
}

func (fn *writeFn) flush(ctx context.Context) error {
	if _, err := fn.pipeline.Exec(ctx); err != nil {
		return fmt.Errorf("error executing commands: %w", err)
	}

	fn.pipeline = nil
	fn.batchCount = 0

	return nil
}
