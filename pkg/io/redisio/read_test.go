package redisio

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"

	"github.com/johannaojeling/go-beam-pipeline/pkg/internal/testutils/redis"
)

func TestRead(t *testing.T) {
	type entry struct {
		Field1 string `json:"field1"`
		Field2 int    `json:"field2"`
	}

	testCases := []struct {
		reason      string
		keyPatterns []string
		elemType    reflect.Type
		input       map[string]string
		expected    []interface{}
	}{
		{
			reason:      "Should read from Redis to PCollection of type entry",
			keyPatterns: []string{"key*"},
			elemType:    reflect.TypeOf(entry{}),
			input: map[string]string{
				"key1":           `{"field1":"val1","field2":1}`,
				"key2":           `{"field1":"val2","field2":2}`,
				"nonMatchingKey": `{"field1":"val3","field2":3}`,
			},
			expected: []interface{}{
				entry{
					Field1: "val1",
					Field2: 1,
				},
				entry{
					Field1: "val2",
					Field2: 2,
				},
			},
		},
		{
			reason:      "Should read from Redis to PCollection of type map[string]interface{}",
			keyPatterns: []string{"key*"},
			elemType:    reflect.TypeOf(map[string]interface{}{}),
			input: map[string]string{
				"key1":           `{"field1":"val1","field2":1}`,
				"key2":           `{"field1":"val2","field2":2}`,
				"nonMatchingKey": `{"field1":"val3","field2":3}`,
			},
			expected: []interface{}{
				map[string]interface{}{
					"field1": "val1",
					"field2": 1,
				},
				map[string]interface{}{
					"field1": "val2",
					"field2": 2,
				},
			},
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("Test %d: %s", i, tc.reason), func(t *testing.T) {
			miniRedis, err := miniredis.Run()
			if err != nil {
				t.Fatalf("error initializing Miniredis: %v", err)
			}
			defer miniRedis.Close()

			address := miniRedis.Addr()
			url := fmt.Sprintf("redis://%s/0", address)

			err = redis.SetValues(url, tc.input)
			if err != nil {
				t.Fatalf("error setting Redis values: %v", err)
			}

			beam.Init()
			pipeline, scope := beam.NewPipelineWithRoot()

			actual := Read(scope, url, tc.keyPatterns, tc.elemType)

			passert.Equals(scope, actual, tc.expected...)
			ptest.RunAndValidate(t, pipeline)
		})
	}
}
