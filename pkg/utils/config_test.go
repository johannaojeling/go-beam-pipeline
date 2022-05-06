package utils

import (
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/johannaojeling/go-beam-pipeline/pkg/pipeline"
	"github.com/johannaojeling/go-beam-pipeline/pkg/pipeline/sink"
	sinkfile "github.com/johannaojeling/go-beam-pipeline/pkg/pipeline/sink/file"
	"github.com/johannaojeling/go-beam-pipeline/pkg/pipeline/source"
	sourcefile "github.com/johannaojeling/go-beam-pipeline/pkg/pipeline/source/file"
)

func TestParseConfig(t *testing.T) {
	t.Run("Should parse yaml config with templated fields to Options", func(t *testing.T) {
		content, err := ioutil.ReadFile("./testdata/config.yaml")
		if err != nil {
			t.Fatalf("error reading config file: %v", err)
		}
		fields := struct {
			Bucket string
		}{
			Bucket: "test-bucket",
		}
		var actual pipeline.Options
		err = ParseConfig(string(content), fields, &actual)

		expected := pipeline.Options{
			Source: source.Source{
				Format: "FILE",
				File: sourcefile.File{
					Format: "JSON",
					Path:   "gs://test-bucket/input.json",
				},
			},
			Sink: sink.Sink{
				Format: "FILE",
				File: sinkfile.File{
					Format: "JSON",
					Path:   "gs://test-bucket/output.json",
				},
			},
		}

		assert.NoError(t, err, "Error should be nil")
		assert.Equal(t, expected, actual, "Options should match")
	})
}
