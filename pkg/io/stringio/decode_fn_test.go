package stringio

import (
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
)

func TestDecodeFn_ProcessElement(t *testing.T) {
	t.Run("Should decode bytes to string", func(t *testing.T) {
		beam.Init()
		pipeline, scope := beam.NewPipelineWithRoot()

		input := []byte("test")
		col := beam.Create(scope, input)
		actual := beam.ParDo(
			scope,
			&DecodeFn{},
			col,
		)

		expected := "test"
		passert.Equals(scope, actual, expected)
		ptest.RunAndValidate(t, pipeline)
	})
}
