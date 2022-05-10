package file

import (
	"context"
	"testing"

	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/io/filesystem/local"
	"github.com/stretchr/testify/assert"
)

func TestReadFile(t *testing.T) {
	t.Run("Should read file", func(t *testing.T) {
		ctx := context.Background()
		path := "./testdata/input.txt"

		actual, err := ReadFile(ctx, path)

		expected := []byte("test\n")

		assert.NoError(t, err, "Error should be nil")
		assert.Equal(t, expected, actual, "Content should match")
	})
}
