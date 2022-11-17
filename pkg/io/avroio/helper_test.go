package avroio

import (
	"bytes"
	"encoding/json"
	"os"
	"testing"

	"github.com/linkedin/goavro"
)

func ReadAvro(t *testing.T, path string) []map[string]any {
	t.Helper()

	avroBytes, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("error reading file: %v", err)
	}

	reader := bytes.NewReader(avroBytes)

	ocfReader, err := goavro.NewOCFReader(reader)
	if err != nil {
		t.Fatalf("error initializing OCF reader: %v", err)
	}

	records := make([]map[string]any, 0)

	for ocfReader.Scan() {
		datum, err := ocfReader.Read()
		if err != nil {
			t.Fatalf("error reading datum: %v", err)
		}

		jsonBytes, err := json.Marshal(datum)
		if err != nil {
			t.Fatalf("error marshaling datum to json: %v", err)
		}

		var record map[string]any
		if err := json.Unmarshal(jsonBytes, &record); err != nil {
			t.Fatalf("error unmarshaling json to map: %v", err)
		}

		records = append(records, record)
	}

	if err := ocfReader.Err(); err != nil {
		t.Fatalf("error from OCF reader: %v", err)
	}

	return records
}
