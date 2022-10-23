package mongodbio

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
	"github.com/stretchr/testify/suite"

	"github.com/johannaojeling/go-beam-pipeline/pkg/internal/testutils/mongodbutils"
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
		Key1 string `bson:"key1"`
		Key2 int    `bson:"key2"`
	}

	testCases := []struct {
		reason   string
		filter   string
		elemType reflect.Type
		records  []map[string]any
		expected []any
	}{
		{
			reason:   "Should read all documents from MongoDB to PCollection of type doc",
			elemType: reflect.TypeOf(doc{}),
			records: []map[string]any{
				{"key1": "val1", "key2": 1},
				{"key1": "val2", "key2": 2},
			},
			expected: []any{doc{Key1: "val1", Key2: 1}, doc{Key1: "val2", Key2: 2}},
		},
		{
			reason:   "Should read documents from MongoDB where filter match to PCollection of type doc",
			filter:   "{\"key2\": {\"$gt\": 1}}",
			elemType: reflect.TypeOf(doc{}),
			records: []map[string]any{
				{"key1": "val1", "key2": 1},
				{"key1": "val2", "key2": 2},
				{"key1": "val3", "key2": 3},
			},
			expected: []any{doc{Key1: "val2", Key2: 2}, doc{Key1: "val3", Key2: 3}},
		},
	}

	for i, tc := range testCases {
		s.T().Run(fmt.Sprintf("Test %d: %s", i, tc.reason), func(t *testing.T) {
			database := "testdatabase"
			collection := "testcollection"

			cfg := ReadConfig{
				URL:        s.URL,
				Database:   database,
				Collection: collection,
				Filter:     tc.filter,
			}

			ctx := context.Background()
			client, err := mongodbutils.NewClient(ctx, s.URL)
			if err != nil {
				t.Fatalf("error initializing client: %v", err)
			}
			defer client.Disconnect(ctx) //nolint:errcheck

			testCollection := client.Database(database).Collection(collection)
			err = mongodbutils.WriteDocuments(ctx, testCollection, tc.records)
			if err != nil {
				t.Fatalf("error writing records to collection %v", err)
			}

			beam.Init()
			pipeline, scope := beam.NewPipelineWithRoot()

			actual := Read(scope, cfg, tc.elemType)

			passert.Equals(scope, actual, tc.expected...)
			ptest.RunAndValidate(t, pipeline)

			s.TearDownTest(ctx, testCollection)
		})
	}
}
