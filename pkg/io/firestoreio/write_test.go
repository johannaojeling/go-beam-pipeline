package firestoreio

import (
	"fmt"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"

	"github.com/johannaojeling/go-beam-pipeline/pkg/internal/testutils/firestoreutils"
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

	testCases := []struct {
		reason   string
		input    []any
		expected []map[string]any
	}{
		{
			reason: "Should write to Firestore collection from PCollection of type doc",
			input:  []any{doc{Key: "val1"}, doc{Key: "val2"}},
			expected: []map[string]any{
				{"key": "val1"},
				{"key": "val2"},
			},
		},
		{
			reason: "Should write to Firestore collection from PCollection of type map",
			input: []any{
				map[string]any{"key": "val1"},
				map[string]any{"key": "val2"},
			},
			expected: []map[string]any{
				{"key": "val1"},
				{"key": "val2"},
			},
		},
	}

	for i, tc := range testCases {
		s.T().Run(fmt.Sprintf("Test %d: %s", i, tc.reason), func(t *testing.T) {
			project := testProject
			collection := "docs"

			cfg := WriteConfig{
				Project:    project,
				Collection: collection,
			}

			beam.Init()
			pipeline, scope := beam.NewPipelineWithRoot()

			col := beam.Create(scope, tc.input...)
			Write(scope, cfg, col)

			ptest.RunAndValidate(t, pipeline)

			actual, err := firestoreutils.ReadDocuments(project, collection)
			if err != nil {
				t.Fatalf("error reading documents %v", err)
			}

			assert.ElementsMatch(t, tc.expected, actual, "Elements should match in any order")
			s.TearDownTest()
		})
	}
}
