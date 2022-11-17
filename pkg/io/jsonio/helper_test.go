package jsonio

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/johannaojeling/go-beam-pipeline/pkg/internal/testutils"
)

func ReadJSON(t *testing.T, path string) []map[string]any {
	t.Helper()

	lines := readLines(t, path)
	records := make([]map[string]any, len(lines))

	for i, line := range lines {
		var record map[string]any
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatalf("error unmarhaling json to map: %v", err)
		}

		records[i] = record
	}

	return records
}

func readLines(t *testing.T, path string) []string {
	t.Helper()

	content := testutils.ReadText(t, path)

	trimmed := strings.TrimRight(content, "\n")
	lines := strings.Split(trimmed, "\n")

	return lines
}
