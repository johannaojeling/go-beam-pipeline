package csvio

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
	type user struct {
		Key string `json:"key"`
	}

	expectedPath := "./testdata/expected.csv"

	testCases := []struct {
		reason string
		input  []interface{}
	}{
		{
			reason: "Should write to json file from PCollection of type user",
			input:  []interface{}{user{Key: "val1"}, user{Key: "val2"}},
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

			actual, err := fileutils.ReadText(path)
			if err != nil {
				t.Fatalf("error reading output file %v", err)
			}

			expected, err := fileutils.ReadText(expectedPath)
			if err != nil {
				t.Fatalf("error reading expected file %v", err)
			}

			assert.Equal(t, expected, actual)
		})
	}
}
