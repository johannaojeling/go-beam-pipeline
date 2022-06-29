package csv

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMarshal(t *testing.T) {
	type entry struct {
		BoolField   bool
		StringField string
		IntField    int
		UIntField   uint
		FloatField  float64
	}

	testCases := []struct {
		reason     string
		input      any
		expected   string
		expectsErr bool
	}{
		{
			reason: "Should parse csv string to type entry",
			input: entry{
				BoolField:   true,
				StringField: "test",
				IntField:    123,
				UIntField:   2,
				FloatField:  123.456,
			},
			expected:   "true,test,123,2,123.456",
			expectsErr: false,
		},
		{
			reason: "Should return error as input is not a struct",
			input: map[string]any{
				"bool_field":   true,
				"string_field": "test",
				"int_field":    123,
				"uint_field":   2,
				"float_field":  123.456,
			},
			expected:   "",
			expectsErr: true,
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("Test %d: %s", i, tc.reason), func(t *testing.T) {
			actual, err := Marshal(tc.input)

			assert.Equal(t, tc.expectsErr, err != nil, "Error should match")
			assert.Equal(t, tc.expected, actual, "Csv line should match")
		})
	}
}
