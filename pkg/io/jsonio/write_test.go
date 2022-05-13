package jsonio

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
	type entry struct {
		Key string `json:"key"`
	}

	expectedPath := "./testdata/expected.json"

	testCases := []struct {
		reason string
		input  []interface{}
	}{
		{
			reason: "Should write to json file from PCollection of type entry",
			input:  []interface{}{entry{Key: "val1"}, entry{Key: "val2"}},
		},
		{
			reason: "Should write to json file from PCollection of type map",
			input: []interface{}{
				map[string]interface{}{"key": "val1"},
				map[string]interface{}{"key": "val2"},
			},
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("Test %d: %s", i, tc.reason), func(t *testing.T) {
			tempDir := t.TempDir()
			outputPath := filepath.Join(tempDir, "output.json")

			beam.Init()
			pipeline, scope := beam.NewPipelineWithRoot()

			col := beam.Create(scope, tc.input...)
			Write(scope, outputPath, col)

			ptest.RunAndValidate(t, pipeline)

			actual, err := testutils.ReadJson(outputPath)
			if err != nil {
				t.Fatalf("error reading output file %v", err)
			}

			expected, err := testutils.ReadJson(expectedPath)
			if err != nil {
				t.Fatalf("error reading expected file %v", err)
			}

			assert.Equal(t, expected, actual)
		})
	}
}
