package csv

import (
	"encoding/csv"
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

func Unmarshal(line string, out any) error {
	structValPtr := reflect.ValueOf(out)

	structValPtrKind := structValPtr.Kind()
	if structValPtrKind != reflect.Ptr {
		return fmt.Errorf("out must be a pointer but was: %v", structValPtrKind)
	}

	structVal := structValPtr.Elem()

	structValKind := structVal.Kind()
	if structValKind != reflect.Struct {
		return fmt.Errorf("out must be a pointer to a struct but was: %v", structValKind)
	}

	row, err := convertToRow(line)
	if err != nil {
		return err
	}

	structType := structVal.Type()
	outVal := reflect.New(structType).Elem()

	err = unmarshalRow(row, outVal)
	if err != nil {
		return fmt.Errorf("error unmarshaling row to out value: %v", err)
	}

	structVal.Set(outVal)

	return nil
}

func convertToRow(line string) ([]string, error) {
	stringReader := strings.NewReader(line)
	csvReader := csv.NewReader(stringReader)

	row, err := csvReader.Read()
	if err != nil {
		return nil, fmt.Errorf("error reading line to csv row: %v", err)
	}
	return row, nil
}

func unmarshalRow(row []string, val reflect.Value) error {
	for i := 0; i < val.NumField(); i++ {
		fieldVal := val.Field(i)
		rowVal := row[i]

		err := parseRowValue(rowVal, fieldVal)
		if err != nil {
			return fmt.Errorf("error parsing row value: %v", err)
		}
	}
	return nil
}

func parseRowValue(rowVal string, fieldVal reflect.Value) error {
	switch kind := fieldVal.Kind(); kind {
	case reflect.String:
		fieldVal.SetString(rowVal)

	case reflect.Bool:
		parsedVal, err := strconv.ParseBool(rowVal)
		if err != nil {
			return fmt.Errorf("error parsing row value to bool: %v", err)
		}

		fieldVal.SetBool(parsedVal)

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		parsedVal, err := strconv.ParseInt(rowVal, 10, 64)
		if err != nil {
			return fmt.Errorf("error parsing row value to int: %v", err)
		}

		fieldVal.SetInt(parsedVal)

	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		parsedVal, err := strconv.ParseUint(rowVal, 10, 64)
		if err != nil {
			return fmt.Errorf("error parsing row value to uint: %v", err)
		}

		fieldVal.SetUint(parsedVal)

	case reflect.Float32, reflect.Float64:
		parsedVal, err := strconv.ParseFloat(rowVal, 64)
		if err != nil {
			return fmt.Errorf("error parsing row value to float: %v", err)
		}

		fieldVal.SetFloat(parsedVal)

	default:
		return fmt.Errorf("unable to parse row value of kind %v", kind)
	}
	return nil
}
