package firestoreio

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/johannaojeling/go-beam-pipeline/pkg/internal/testutils/firestore"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
	"github.com/stretchr/testify/suite"
)

type ReadSuite struct {
	Suite
}

func TestReadSuite(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	suite.Run(t, new(ReadSuite))
}

func (s *ReadSuite) TestRead() {
	type doc struct {
		Key string `firestore:"key"`
	}

	testCases := []struct {
		reason   string
		elemType reflect.Type
		records  []map[string]interface{}
		expected []interface{}
	}{
		{
			reason:   "Should read from Firestore collection to PCollection of type doc",
			elemType: reflect.TypeOf(doc{}),
			records: []map[string]interface{}{
				{"key": "val1"},
				{"key": "val2"},
			},
			expected: []interface{}{doc{Key: "val1"}, doc{Key: "val2"}},
		},
		{
			reason:   "Should read from Firestore collection to PCollection of type map",
			elemType: reflect.TypeOf(map[string]interface{}{}),
			records: []map[string]interface{}{
				{"key": "val1"},
				{"key": "val2"},
			},
			expected: []interface{}{
				map[string]interface{}{"key": "val1"},
				map[string]interface{}{"key": "val2"},
			},
		},
	}

	for i, tc := range testCases {
		s.T().Run(fmt.Sprintf("Test %d: %s", i, tc.reason), func(t *testing.T) {
			project := TestProject
			collection := "docs"

			cfg := ReadConfig{
				Project:    project,
				Collection: collection,
			}

			err := firestore.WriteDocuments(project, collection, tc.records)
			if err != nil {
				t.Fatalf("error writing records to collection %v", err)
			}

			beam.Init()
			pipeline, scope := beam.NewPipelineWithRoot()

			actual := Read(scope, cfg, tc.elemType)

			passert.Equals(scope, actual, tc.expected...)
			ptest.RunAndValidate(t, pipeline)

			s.TearDownTest()
		})
	}
}
