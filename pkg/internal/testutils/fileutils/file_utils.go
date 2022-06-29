package fileutils

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strings"

	"github.com/linkedin/goavro"
)

func ReadAvro(path string) ([]map[string]any, error) {
	avroBytes, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("error reading file: %v", err)
	}

	reader := bytes.NewReader(avroBytes)
	ocfReader, err := goavro.NewOCFReader(reader)
	if err != nil {
		return nil, fmt.Errorf("error initializing OCF reader: %v", err)
	}

	records := make([]map[string]any, 0)

	for ocfReader.Scan() {
		datum, err := ocfReader.Read()
		if err != nil {
			return nil, fmt.Errorf("error reading datum: %v", err)
		}

		jsonBytes, err := json.Marshal(datum)
		if err != nil {
			return nil, fmt.Errorf("error marshaling datum to json: %v", err)
		}

		var record map[string]any
		err = json.Unmarshal(jsonBytes, &record)
		if err != nil {
			return nil, fmt.Errorf("error unmarshaling json to map: %v", err)
		}

		records = append(records, record)
	}

	err = ocfReader.Err()
	if err != nil {
		return nil, fmt.Errorf("error from OCF reader: %v", err)
	}

	return records, nil
}

func ReadJson(path string) ([]map[string]any, error) {
	lines, err := ReadLines(path)
	if err != nil {
		return nil, fmt.Errorf("error reading lines: %v", err)
	}

	records := make([]map[string]any, len(lines))

	for _, line := range lines {
		var record map[string]any
		err = json.Unmarshal([]byte(line), &record)
		if err != nil {
			return nil, fmt.Errorf("error unmarhaling json to map: %v", err)
		}

		records = append(records, record)
	}

	return records, nil
}

func ReadLines(path string) ([]string, error) {
	content, err := ReadText(path)
	if err != nil {
		return nil, fmt.Errorf("error reading text: %v", err)
	}

	trimmed := strings.TrimRight(content, "\n")
	lines := strings.Split(trimmed, "\n")

	return lines, nil
}

func ReadText(path string) (string, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("error reading file: %v", err)
	}
	return string(content), nil
}

func WriteText(path string, content string) error {
	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("error creating file: %v", err)
	}

	defer file.Close()

	_, err = file.WriteString(content)
	if err != nil {
		return fmt.Errorf("error writing to file: %v", err)
	}

	err = file.Sync()
	if err != nil {
		return fmt.Errorf("error syncing with file: %v", err)
	}

	return nil
}
