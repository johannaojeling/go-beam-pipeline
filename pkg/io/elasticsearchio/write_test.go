package elasticsearchio

import (
	"context"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"

	"github.com/johannaojeling/go-beam-pipeline/pkg/internal/testutils/es"
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
		Key string `json:"key"`
	}

	s.T().Run("Should write to Elasticsearch", func(t *testing.T) {
		addresses := []string{s.URL}
		index := "testindex"
		cloudId := ""
		apiKey := ""
		flushBytes := 0

		input := []interface{}{doc{Key: "val1"}, doc{Key: "val2"}}

		beam.Init()
		pipeline, scope := beam.NewPipelineWithRoot()

		col := beam.Create(scope, input...)
		Write(scope, addresses, cloudId, apiKey, index, flushBytes, col)

		ptest.RunAndValidate(t, pipeline)

		cfg := elasticsearch.Config{Addresses: addresses}
		client, err := elasticsearch.NewClient(cfg)
		if err != nil {
			t.Fatalf("failed to initialize client: %v", err)
		}

		ctx := context.Background()
		err = es.RefreshIndices(ctx, client, []string{index})
		if err != nil {
			t.Fatalf("failed to refresh index: %v", err)
		}

		query := `{"match_all": {}}`
		actual, err := es.ReadDocuments(ctx, client, index, query)
		if err != nil {
			t.Fatalf("failed to read documents %v", err)
		}
		expected := []map[string]interface{}{
			{"key": "val1"},
			{"key": "val2"},
		}

		assert.ElementsMatch(t, expected, actual, "Elements should match in any order")

		s.TearDownTest(ctx, client, index)
	})
}
