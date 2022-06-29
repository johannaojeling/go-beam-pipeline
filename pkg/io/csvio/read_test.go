package csvio

import (
	"fmt"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/io/filesystem/local"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"

	"github.com/johannaojeling/go-beam-pipeline/pkg/internal/testutils/fileutils"
)

func TestRead(t *testing.T) {
	type user struct {
		Key string `json:"key"`
	}

	testCases := []struct {
		reason   string
		elemType reflect.Type
		input    string
		expected []any
	}{
		{
			reason:   "Should read from csv file to PCollection of type user",
			elemType: reflect.TypeOf(user{}),
			input:    "val1\nval2\n",
			expected: []any{user{Key: "val1"}, user{Key: "val2"}},
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("Test %d: %s", i, tc.reason), func(t *testing.T) {
			tempDir := t.TempDir()
			path := filepath.Join(tempDir, "input.csv")

			err := fileutils.WriteText(path, tc.input)
			if err != nil {
				t.Fatalf("error writing input %v", err)
			}

			beam.Init()
			pipeline, scope := beam.NewPipelineWithRoot()

			actual := Read(scope, path, tc.elemType)

			passert.Equals(scope, actual, tc.expected...)
			ptest.RunAndValidate(t, pipeline)
		})
	}
}
