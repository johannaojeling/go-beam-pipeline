package fileutils

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/linkedin/goavro"
)

func ReadAvro(path string) ([]map[string]any, error) {
	avroBytes, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("error reading file: %w", err)
	}

	reader := bytes.NewReader(avroBytes)

	ocfReader, err := goavro.NewOCFReader(reader)
	if err != nil {
		return nil, fmt.Errorf("error initializing OCF reader: %w", err)
	}

	records := make([]map[string]any, 0)

	for ocfReader.Scan() {
		datum, err := ocfReader.Read()
		if err != nil {
			return nil, fmt.Errorf("error reading datum: %w", err)
		}

		jsonBytes, err := json.Marshal(datum)
		if err != nil {
			return nil, fmt.Errorf("error marshaling datum to json: %w", err)
		}

		var record map[string]any
		if err := json.Unmarshal(jsonBytes, &record); err != nil {
			return nil, fmt.Errorf("error unmarshaling json to map: %w", err)
		}

		records = append(records, record)
	}

	if err := ocfReader.Err(); err != nil {
		return nil, fmt.Errorf("error from OCF reader: %w", err)
	}

	return records, nil
}

func ReadJSON(path string) ([]map[string]any, error) {
	lines, err := ReadLines(path)
	if err != nil {
		return nil, fmt.Errorf("error reading lines: %w", err)
	}

	records := make([]map[string]any, len(lines))

	for i, line := range lines {
		var record map[string]any
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			return nil, fmt.Errorf("error unmarhaling json to map: %w", err)
		}

		records[i] = record
	}

	return records, nil
}

func ReadLines(path string) ([]string, error) {
	content, err := ReadText(path)
	if err != nil {
		return nil, fmt.Errorf("error reading text: %w", err)
	}

	trimmed := strings.TrimRight(content, "\n")
	lines := strings.Split(trimmed, "\n")

	return lines, nil
}

func ReadText(path string) (string, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("error reading file: %w", err)
	}

	return string(content), nil
}

func WriteText(path string, content string) error {
	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("error creating file: %w", err)
	}

	defer file.Close()

	if _, err = file.WriteString(content); err != nil {
		return fmt.Errorf("error writing to file: %w", err)
	}

	if err := file.Sync(); err != nil {
		return fmt.Errorf("error syncing with file: %w", err)
	}

	return nil
}
