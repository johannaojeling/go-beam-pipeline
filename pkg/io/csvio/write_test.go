package csvio

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/io/filesystem/local"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
	"github.com/stretchr/testify/assert"

	"github.com/johannaojeling/go-beam-pipeline/pkg/internal/testutils"
)

func TestWrite(t *testing.T) {
	type user struct {
		Key string `json:"key"`
	}

	expectedPath := "./testdata/expected.csv"

	testCases := []struct {
		reason string
		input  []any
	}{
		{
			reason: "Should write to json file from PCollection of type user",
			input:  []any{user{Key: "val1"}, user{Key: "val2"}},
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("Test %d: %s", i, tc.reason), func(t *testing.T) {
			tempDir := t.TempDir()
			path := filepath.Join(tempDir, "expected.csv")

			beam.Init()
			pipeline, scope := beam.NewPipelineWithRoot()

			col := beam.Create(scope, tc.input...)
			Write(scope, path, col)

			ptest.RunAndValidate(t, pipeline)

			actual := testutils.ReadText(t, path)
			expected := testutils.ReadText(t, expectedPath)

			assert.Equal(t, expected, actual)
		})
	}
}
