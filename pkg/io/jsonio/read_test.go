package jsonio

import (
	"fmt"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/io/filesystem/local"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"

	"github.com/johannaojeling/go-beam-pipeline/pkg/internal/testutils"
)

func TestRead(t *testing.T) {
	type entry struct {
		Key string `json:"key"`
	}

	testCases := []struct {
		reason   string
		elemType reflect.Type
		input    string
		expected []any
	}{
		{
			reason:   "Should read from json file to PCollection of type entry",
			elemType: reflect.TypeOf(entry{}),
			input:    "{\"key\":\"val1\"}\n{\"key\":\"val2\"}\n",
			expected: []any{entry{Key: "val1"}, entry{Key: "val2"}},
		},
		{
			reason:   "Should read from json file to PCollection of type map",
			elemType: reflect.TypeOf(map[string]any{}),
			input:    "{\"key\":\"val1\"}\n{\"key\":\"val2\"}\n",
			expected: []any{
				map[string]any{"key": "val1"},
				map[string]any{"key": "val2"},
			},
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("Test %d: %s", i, tc.reason), func(t *testing.T) {
			tempDir := t.TempDir()
			path := filepath.Join(tempDir, "input.json")

			testutils.WriteText(t, path, tc.input)

			beam.Init()
			pipeline, scope := beam.NewPipelineWithRoot()

			actual := Read(scope, path, tc.elemType)

			passert.Equals(scope, actual, tc.expected...)
			ptest.RunAndValidate(t, pipeline)
		})
	}
}
