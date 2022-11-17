package jsonio

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
)

func TestUnMarshalFn_ProcessElement(t *testing.T) {
	type user struct {
		ID   int    `json:"id"`
		Name string `json:"name"`
	}

	testCases := []struct {
		reason   string
		elemType reflect.Type
		input    []byte
		expected any
	}{
		{
			reason:   "Should parse json string to map",
			elemType: reflect.TypeOf(map[string]any{}),
			input:    []byte(`{"id":1,"name":"user1"}`),
			expected: map[string]any{
				"id":   1,
				"name": "user1",
			},
		},
		{
			reason:   "Should parse json string to entry",
			elemType: reflect.TypeOf(user{}),
			input:    []byte(`{"id":1,"name":"user1"}`),
			expected: user{
				ID:   1,
				Name: "user1",
			},
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("Test %d: %s", i, tc.reason), func(t *testing.T) {
			beam.Init()
			pipeline, scope := beam.NewPipelineWithRoot()

			col := beam.Create(scope, tc.input)
			actual := beam.ParDo(
				scope,
				NewUnMarshalFn(tc.elemType),
				col,
				beam.TypeDefinition{Var: beam.XType, T: tc.elemType},
			)

			passert.Equals(scope, actual, tc.expected)
			ptest.RunAndValidate(t, pipeline)
		})
	}
}
