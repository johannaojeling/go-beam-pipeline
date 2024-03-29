package elasticsearchio

import (
	"context"
	"fmt"
	"reflect"
	"testing"

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
		t.Skip("skipping long-running integration test")
	}

	suite.Run(t, &ReadSuite{})
}

func (s *ReadSuite) TestRead() {
	type doc struct {
		Key string `json:"key"`
	}

	testCases := []struct {
		reason   string
		input    []map[string]any
		elemType reflect.Type
		expected []any
	}{
		{
			reason: "Should read from Elasticsearch",
			input: []map[string]any{
				{"key": "val1"},
				{"key": "val2"},
			},
			elemType: reflect.TypeOf(doc{}),
			expected: []any{
				doc{Key: "val1"},
				doc{Key: "val2"},
			},
		},
	}

	for i, tc := range testCases {
		s.T().Run(fmt.Sprintf("Test %d: %s", i, tc.reason), func(t *testing.T) {
			addresses := []string{s.URL}
			index := "testindex"
			readCfg := ReadConfig{
				Addresses: addresses,
				Index:     index,
				Query:     `{"match_all": {}}`,
				KeepAlive: "1m",
			}

			ctx := context.Background()
			client := NewClient(t, addresses)

			IndexDocuments(ctx, t, client, index, tc.input)
			RefreshIndices(ctx, t, client, []string{index})

			beam.Init()
			pipeline, scope := beam.NewPipelineWithRoot()

			actual := Read(scope, readCfg, tc.elemType)

			passert.Equals(scope, actual, tc.expected...)
			ptest.RunAndValidate(t, pipeline)

			DeleteIndices(ctx, t, client, []string{index})
		})
	}
}
