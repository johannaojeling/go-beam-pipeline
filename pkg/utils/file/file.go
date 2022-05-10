package file

import (
	"context"
	"fmt"
	"io/ioutil"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/filesystem"
)

func ReadFile(ctx context.Context, path string) ([]byte, error) {
	fs, err := filesystem.New(ctx, path)
	if err != nil {
		return nil, fmt.Errorf("error initializing filesystem: %v", err)
	}
	defer fs.Close()

	reader, err := fs.OpenRead(ctx, path)
	if err != nil {
		return nil, fmt.Errorf("error opening file for reading: %v", err)
	}
	defer reader.Close()

	return ioutil.ReadAll(reader)
}
