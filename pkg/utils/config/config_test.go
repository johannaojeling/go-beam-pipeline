package config

import (
	"os"
	"testing"

	"github.com/johannaojeling/go-beam-pipeline/pkg/options"
	"github.com/stretchr/testify/assert"
)

func TestParseConfig(t *testing.T) {
	t.Run("Should parse yaml config with templated fields to PipelineOption", func(t *testing.T) {
		content, err := os.ReadFile("./testdata/config.yaml")
		if err != nil {
			t.Fatalf("error reading config file: %v", err)
		}

		fields := struct {
			Bucket string
		}{
			Bucket: "test-bucket",
		}
		var actual options.PipelineOption
		err = ParseConfig(string(content), fields, &actual)

		expected := options.PipelineOption{
			Source: options.SourceOption{
				Format: "FILE",
				File: options.FileReadOption{
					Format: "JSON",
					Path:   "gs://test-bucket/input.json",
				},
			},
			Sink: options.SinkOption{
				Format: "FILE",
				File: options.FileWriteOption{
					Format: "JSON",
					Path:   "gs://test-bucket/output.json",
				},
			},
		}

		assert.NoError(t, err, "Error should be nil")
		assert.Equal(t, expected, actual, "PipelineOption should match")
	})
}
