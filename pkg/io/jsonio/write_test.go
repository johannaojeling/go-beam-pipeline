package jsonio

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/io/filesystem/local"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
	"github.com/stretchr/testify/assert"

	"github.com/johannaojeling/go-beam-pipeline/pkg/internal/testutils/fileutils"
)

func TestWrite(t *testing.T) {
	type entry struct {
		Key string `json:"key"`
	}

	expectedPath := "./testdata/expected.json"

	testCases := []struct {
		reason string
		input  []any
	}{
		{
			reason: "Should write to json file from PCollection of type entry",
			input:  []any{entry{Key: "val1"}, entry{Key: "val2"}},
		},
		{
			reason: "Should write to json file from PCollection of type map",
			input: []any{
				map[string]any{"key": "val1"},
				map[string]any{"key": "val2"},
			},
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("Test %d: %s", i, tc.reason), func(t *testing.T) {
			tempDir := t.TempDir()
			path := filepath.Join(tempDir, "output.json")

			beam.Init()
			pipeline, scope := beam.NewPipelineWithRoot()

			col := beam.Create(scope, tc.input...)
			Write(scope, path, col)

			ptest.RunAndValidate(t, pipeline)

			actual, err := fileutils.ReadJson(path)
			if err != nil {
				t.Fatalf("error reading output file %v", err)
			}

			expected, err := fileutils.ReadJson(expectedPath)
			if err != nil {
				t.Fatalf("error reading expected file %v", err)
			}

			assert.Equal(t, expected, actual)
		})
	}
}
