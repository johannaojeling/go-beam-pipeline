package csv

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

func Marshal(val any) (string, error) {
	structVal := reflect.ValueOf(val)

	structValKind := structVal.Kind()
	if structValKind != reflect.Struct {
		return "", fmt.Errorf("value must be a struct but was: %v", structValKind)
	}

	row, err := marshalRow(structVal)
	if err != nil {
		return "", fmt.Errorf("error marshaling value to row: %w", err)
	}

	line, err := convertToLine(row)
	if err != nil {
		return "", err
	}

	return line, nil
}

func marshalRow(val reflect.Value) ([]string, error) {
	numFields := val.NumField()
	row := make([]string, 0, numFields)

	for i := 0; i < numFields; i++ {
		fieldVal := val.Field(i)

		rowVal, err := parseFieldValue(fieldVal)
		if err != nil {
			return nil, err
		}

		row = append(row, rowVal)
	}

	return row, nil
}

func parseFieldValue(fieldVal reflect.Value) (string, error) {
	switch kind := fieldVal.Kind(); kind {
	case reflect.String:
		return fieldVal.String(), nil

	case reflect.Bool:
		return strconv.FormatBool(fieldVal.Bool()), nil

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.FormatInt(fieldVal.Int(), 10), nil

	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return strconv.FormatUint(fieldVal.Uint(), 10), nil

	case reflect.Float32, reflect.Float64:
		return strconv.FormatFloat(fieldVal.Float(), 'f', -1, 64), nil

	default:
		return "", fmt.Errorf("unable to parse value of kind %v", kind)
	}
}

func convertToLine(row []string) (string, error) {
	buffer := &bytes.Buffer{}
	csvWriter := csv.NewWriter(buffer)

	if err := csvWriter.Write(row); err != nil {
		return "", fmt.Errorf("error writing csv row to line: %w", err)
	}

	csvWriter.Flush()

	line := buffer.String()
	trimmed := strings.TrimRight(line, "\n")

	return trimmed, nil
}
