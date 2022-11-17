package file

import (
	"context"
	"fmt"
	"io"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/filesystem"
)

func ReadFile(ctx context.Context, path string) ([]byte, error) {
	fs, err := filesystem.New(ctx, path)
	if err != nil {
		return nil, fmt.Errorf("error initializing filesystem: %w", err)
	}

	defer fs.Close()

	reader, err := fs.OpenRead(ctx, path)
	if err != nil {
		return nil, fmt.Errorf("error opening file for reading: %w", err)
	}

	defer reader.Close()

	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("error reading file: %w", err)
	}

	return data, nil
}
