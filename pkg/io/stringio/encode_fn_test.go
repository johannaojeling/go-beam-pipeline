package stringio

import (
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
)

func TestEncodeFn_ProcessElement(t *testing.T) {
	t.Run("Should encode string to bytes", func(t *testing.T) {
		beam.Init()
		pipeline, scope := beam.NewPipelineWithRoot()

		input := "test"
		col := beam.Create(scope, input)
		actual := beam.ParDo(
			scope,
			NewEncodeFn(),
			col,
		)

		expected := []byte("test")
		passert.Equals(scope, actual, expected)
		ptest.RunAndValidate(t, pipeline)
	})
}
