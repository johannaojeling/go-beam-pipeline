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

	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("error writing to file: %v", err)
	}
}
