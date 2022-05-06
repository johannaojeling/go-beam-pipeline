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

	"github.com/johannaojeling/go-beam-pipeline/pkg/internal/testutils"
)

func TestRead(t *testing.T) {
	type user struct {
		Key string `json:"key"`
	}

	testCases := []struct {
		reason   string
		elemType reflect.Type
		input    string
		expected []interface{}
	}{
		{
			reason:   "Should read from csv file to PCollection of type user",
			elemType: reflect.TypeOf(user{}),
			input:    "val1\nval2\n",
			expected: []interface{}{user{Key: "val1"}, user{Key: "val2"}},
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("Test %d: %s", i, tc.reason), func(t *testing.T) {
			tempDir := t.TempDir()
			inputPath := filepath.Join(tempDir, "input.csv")

			err := testutils.WriteText(inputPath, tc.input)
			if err != nil {
				t.Fatalf("error writing input %v", err)
			}

			beam.Init()
			pipeline, scope := beam.NewPipelineWithRoot()

			actual := Read(scope, inputPath, tc.elemType)

			passert.Equals(scope, actual, tc.expected...)
			ptest.RunAndValidate(t, pipeline)
		})
	}
}
