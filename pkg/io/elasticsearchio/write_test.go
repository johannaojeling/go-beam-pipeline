package elasticsearchio

import (
	"context"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
	"github.com/elastic/go-elasticsearch/v8"
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
	suite.Run(t, new(WriteSuite))
}

func (s *WriteSuite) TestWrite() {
	type doc struct {
		Key string `json:"key"`
	}

	s.T().Run("Should write to Elasticsearch", func(t *testing.T) {
		addresses := []string{s.URL}
		index := "testindex"

		writeCfg := WriteConfig{
			Addresses: addresses,
			Index:     index,
		}

		input := []interface{}{doc{Key: "val1"}, doc{Key: "val2"}}

		beam.Init()
		pipeline, scope := beam.NewPipelineWithRoot()

		col := beam.Create(scope, input...)
		Write(scope, writeCfg, col)

		ptest.RunAndValidate(t, pipeline)

		esCfg := elasticsearch.Config{Addresses: addresses}
		client, err := elasticsearch.NewClient(esCfg)
		if err != nil {
			t.Fatalf("failed to initialize client: %v", err)
		}

		ctx := context.Background()
		err = esutils.RefreshIndices(ctx, client, []string{index})
		if err != nil {
			t.Fatalf("failed to refresh index: %v", err)
		}

		query := `{"match_all": {}}`
		actual, err := esutils.SearchDocuments(ctx, client, index, query)
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
