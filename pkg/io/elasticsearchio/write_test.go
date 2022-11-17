package elasticsearchio

import (
	"context"
	"fmt"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"

	"github.com/johannaojeling/go-beam-pipeline/pkg/internal/testutils/esutils"
)

type WriteSuite struct {
	Suite
}

func TestWriteSuite(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	suite.Run(t, &WriteSuite{})
}

func (s *WriteSuite) TestWrite() {
	type doc struct {
		Key string `json:"key"`
	}

	testCases := []struct {
		reason   string
		input    []any
		expected []map[string]any
	}{
		{
			reason: "Should write to Elasticsearch",
			input: []any{
				doc{Key: "val1"},
				doc{Key: "val2"},
			},
			expected: []map[string]any{
				{"key": "val1"},
				{"key": "val2"},
			},
		},
	}

	for i, tc := range testCases {
		s.T().Run(fmt.Sprintf("Test %d: %s", i, tc.reason), func(t *testing.T) {
			addresses := []string{s.URL}
			index := "testindex"

			writeCfg := WriteConfig{
				Addresses: addresses,
				Index:     index,
			}

			beam.Init()
			pipeline, scope := beam.NewPipelineWithRoot()

			col := beam.Create(scope, tc.input...)
			Write(scope, writeCfg, col)

			ptest.RunAndValidate(t, pipeline)

			ctx := context.Background()
			client, err := esutils.NewClient(ctx, addresses)
			if err != nil {
				t.Fatalf("error initializing client: %v", err)
			}

			if err := esutils.RefreshIndices(ctx, client, []string{index}); err != nil {
				t.Fatalf("error refreshing index: %v", err)
			}

			query := `{"match_all": {}}`
			actual, err := esutils.SearchDocuments(ctx, client, index, query)
			if err != nil {
				t.Fatalf("error reading documents %v", err)
			}

			assert.ElementsMatch(t, tc.expected, actual, "Elements should match in any order")

			s.TearDownTest(ctx, client, index)
		})
	}
}
