package csvio

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
)

func TestMarshalFn_ProcessElement(t *testing.T) {
	type user struct {
		Id   string `json:"id"`
		Name string `json:"name"`
	}

	testCases := []struct {
		reason   string
		elemType reflect.Type
		input    interface{}
		expected string
	}{
		{
			reason:   "Should parse element of type user to csv line",
			elemType: reflect.TypeOf(user{}),
			input:    user{Id: "1", Name: "user1"},
			expected: `1,user1`,
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("Test %d: %s", i, tc.reason), func(t *testing.T) {
			beam.Init()
			pipeline, scope := beam.NewPipelineWithRoot()

			col := beam.Create(scope, tc.input)
			actual := beam.ParDo(
				scope,
				NewMarshalFn(tc.elemType),
				col,
			)

			passert.Equals(scope, actual, tc.expected)
			ptest.RunAndValidate(t, pipeline)
		})
	}
}
