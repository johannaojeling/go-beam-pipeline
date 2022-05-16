package redisio

import (
	"context"
	"fmt"
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"

	"github.com/johannaojeling/go-beam-pipeline/pkg/dofns"
	"github.com/johannaojeling/go-beam-pipeline/pkg/io/jsonio"
	"github.com/johannaojeling/go-beam-pipeline/pkg/io/stringio"
)

const DefaultReadBatchSize = 1000

func init() {
	beam.RegisterType(reflect.TypeOf((*readFn)(nil)))
}

func Read(
	scope beam.Scope,
	url string,
	keyPatterns []string,
	elemType reflect.Type,
) beam.PCollection {
	scope = scope.Scope("redisio.Read")
	keyed := ReadKV(scope, url, keyPatterns)
	stringType := reflect.TypeOf("")
	values := beam.ParDo(
		scope,
		&dofns.DropKeyFn{
			XType: beam.EncodedType{T: stringType},
			YType: beam.EncodedType{T: stringType},
		},
		keyed,
	)
	encoded := beam.ParDo(scope, &stringio.EncodeFn{}, values)
	return beam.ParDo(
		scope,
		&jsonio.UnMarshalFn{Type: beam.EncodedType{T: elemType}},
		encoded,
		beam.TypeDefinition{Var: beam.XType, T: elemType},
	)
}

func ReadKV(
	scope beam.Scope,
	url string,
	keyPatterns []string,
) beam.PCollection {
	scope = scope.Scope("redisio.ReadKV")
	col := beam.CreateList(scope, keyPatterns)
	return beam.ParDo(scope, &readFn{redisFn: redisFn{URL: url}}, col)
}

type readFn struct {
	redisFn
	BatchSize int
}

func (fn *readFn) ProcessElement(
	ctx context.Context,
	elem string,
	emit func(string, string),
) error {
	log.Info(ctx, "Reading from Redis")

	batchSize := int64(fn.BatchSize)
	if batchSize <= 0 {
		batchSize = DefaultReadBatchSize
	}

	var cursor uint64
	for {
		var keys []string
		var err error
		keys, cursor, err = fn.client.Scan(ctx, cursor, elem, batchSize).Result()
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
