package firestoreio

import (
	"fmt"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"

	"github.com/johannaojeling/go-beam-pipeline/pkg/internal/testutils"
)

type WriteSuite struct {
	Suite
}

func TestWriteSuite(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	suite.Run(t, new(WriteSuite))
}

func (s *WriteSuite) TestWrite() {
	type doc struct {
		Key string `firestore:"key"`
	}

	project := TestProject
	collection := "docs"

	testCases := []struct {
		reason   string
		input    []interface{}
		expected []map[string]interface{}
	}{
		{
			reason: "Should write to Firestore collection from PCollection of type doc",
			input:  []interface{}{doc{Key: "val1"}, doc{Key: "val2"}},
			expected: []map[string]interface{}{
				{"key": "val1"},
				{"key": "val2"},
			},
		},
		{
			reason: "Should write to Firestore collection from PCollection of type map",
			input: []interface{}{
				map[string]interface{}{"key": "val1"},
				map[string]interface{}{"key": "val2"},
			},
			expected: []map[string]interface{}{
				{"key": "val1"},
				{"key": "val2"},
			},
		},
	}

	for i, tc := range testCases {
		s.T().Run(fmt.Sprintf("Test %d: %s", i, tc.reason), func(t *testing.T) {
			beam.Init()
			pipeline, scope := beam.NewPipelineWithRoot()

			col := beam.Create(scope, tc.input...)
			Write(scope, project, collection, col)

			ptest.RunAndValidate(t, pipeline)

			actual, err := testutils.ReadDocuments(project, collection)
			if err != nil {
				t.Fatalf("error reading output file %v", err)
			}

			assert.ElementsMatch(t, tc.expected, actual, "Elements should match in any order")
			s.TearDownTest()
		})
	}
}
