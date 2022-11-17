package testutils

import (
	"os"
	"testing"
)

func ReadText(t *testing.T, path string) string {
	t.Helper()

	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("error reading file: %v", err)
	}

	return string(content)
}

func WriteText(t *testing.T, path string, content string) {
	t.Helper()

	file, err := os.Create(path)
	if err != nil {
		t.Fatalf("error creating file: %v", err)
	}

	defer file.Close()

	if _, err = file.WriteString(content); err != nil {
		t.Fatalf("error writing to file: %v", err)
	}

	if err := file.Sync(); err != nil {
		t.Fatalf("error syncing with file: %v", err)
	}
}
