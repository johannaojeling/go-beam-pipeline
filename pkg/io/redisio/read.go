package redisio

import (
	"context"
	"fmt"
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"

	"github.com/johannaojeling/go-beam-pipeline/pkg/dofns"
	"github.com/johannaojeling/go-beam-pipeline/pkg/io/jsonio"
	"github.com/johannaojeling/go-beam-pipeline/pkg/io/stringio"
)

const defaultReadBatchSize = 1000

func init() {
	register.DoFn3x1[context.Context, string, func(string, string), error](&readFn{})
	register.Emitter2[string, string]()
}

type ReadConfig struct {
	URL         string
	KeyPatterns []string
	BatchSize   int
}

func Read(
	scope beam.Scope,
	cfg ReadConfig,
	elemType reflect.Type,
) beam.PCollection {
	scope = scope.Scope("redisio.Read")
	keyed := ReadKV(scope, cfg)
	stringType := reflect.TypeOf("")
	values := beam.ParDo(
		scope,
		dofns.NewDropKeyFn(stringType, stringType),
		keyed,
	)
	encoded := beam.ParDo(scope, stringio.NewEncodeFn(), values)
	return beam.ParDo(
		scope,
		jsonio.NewUnMarshalFn(elemType),
		encoded,
		beam.TypeDefinition{Var: beam.XType, T: elemType},
	)
}

func ReadKV(
	scope beam.Scope,
	cfg ReadConfig,
) beam.PCollection {
	scope = scope.Scope("redisio.ReadKV")
	col := beam.CreateList(scope, cfg.KeyPatterns)
	return beam.ParDo(scope, newReadFn(cfg.URL, cfg.BatchSize), col)
}

type readFn struct {
	redisFn
	BatchSize int64
}

func newReadFn(url string, batchSize int) *readFn {
	if batchSize <= 0 {
		batchSize = defaultReadBatchSize
	}

	return &readFn{
		redisFn:   redisFn{URL: url},
		BatchSize: int64(batchSize),
	}
}

func (fn *readFn) ProcessElement(
	ctx context.Context,
	elem string,
	emit func(string, string),
) error {
	log.Info(ctx, "Reading from Redis")

	var cursor uint64
	for {
		var keys []string
		var err error
		keys, cursor, err = fn.client.Scan(ctx, cursor, elem, fn.BatchSize).Result()
		if err != nil {
			return fmt.Errorf("error scanning keys: %v", err)
		}
		if len(keys) == 0 {
			break
		}

		cmd := fn.client.MGet(ctx, keys...)
		values, err := cmd.Result()
		if err != nil {
			return fmt.Errorf("error executing MGET command: %v", err)
		}

		for i := 0; i < len(keys); i++ {
			key := keys[i]
			value := values[i].(string)
			emit(key, value)
		}

		if cursor == 0 {
			break
		}
	}
	return nil
}
