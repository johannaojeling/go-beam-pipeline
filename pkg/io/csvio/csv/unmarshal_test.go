package csv

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUnmarshal(t *testing.T) {
	type entry struct {
		BoolField   bool
		StringField string
		IntField    int
		UIntField   uint
		FloatField  float64
	}

	testCases := []struct {
		reason     string
		input      string
		expected   any
		expectsErr bool
	}{
		{
			reason: "Should parse csv string to type entry",
			input:  `true,test,123,2,123.456`,
			expected: &entry{
				BoolField:   true,
				StringField: "test",
				IntField:    123,
				UIntField:   2,
				FloatField:  123.456,
			},
			expectsErr: false,
		},
		{
			reason:     "Should return error when field types do not match",
			input:      `true,test,123,2,not-a-float`,
			expected:   &entry{},
			expectsErr: true,
		},
		{
			reason:     "Should return error when input is not valid csv",
			input:      `true,test",123,2,123.456`,
			expected:   &entry{},
			expectsErr: true,
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("Test %d: %s", i, tc.reason), func(t *testing.T) {
			actual := &entry{}
			err := Unmarshal(tc.input, actual)

			assert.Equal(t, tc.expectsErr, err != nil, "Error should match")
			assert.Equal(t, tc.expected, actual, "Value should match")
		})
	}
}
