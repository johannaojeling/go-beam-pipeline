package avroio

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/io/filesystem/local"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
	"github.com/stretchr/testify/assert"

	"github.com/johannaojeling/go-beam-pipeline/pkg/internal/testutils/file"
)

func TestWrite(t *testing.T) {
	type entry struct {
		Key string `json:"key"`
	}

	schema := `{
      "type": "record",
      "name": "TestRecord",
      "fields": [
          {"name": "key", "type": "string"}
      ]
    }`
	expectedPath := "./testdata/expected.avro"

	testCases := []struct {
		reason string
		input  []interface{}
	}{
		{
			reason: "Should write to avro file from PCollection of type entry",
			input:  []interface{}{entry{Key: "val1"}, entry{Key: "val2"}},
		},
		{
			reason: "Should write to avro file from PCollection of type map",
			input: []interface{}{
				map[string]interface{}{"key": "val1"},
				map[string]interface{}{"key": "val2"},
			},
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("Test %d: %s", i, tc.reason), func(t *testing.T) {
			tempDir := t.TempDir()
			path := filepath.Join(tempDir, "output.avro")
			config := WriteConfig{
				Path:   path,
				Schema: schema,
			}

			beam.Init()
			pipeline, scope := beam.NewPipelineWithRoot()

			col := beam.Create(scope, tc.input...)
			Write(scope, config, col)

			ptest.RunAndValidate(t, pipeline)

			actual, err := file.ReadAvro(path)
			if err != nil {
				t.Fatalf("error reading output file %v", err)
			}

			expected, err := file.ReadAvro(expectedPath)
			if err != nil {
				t.Fatalf("error reading expected file %v", err)
			}

			assert.Equal(t, expected, actual)
		})
	}
}
