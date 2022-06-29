package elasticsearchio

import (
	"context"
	"reflect"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/stretchr/testify/suite"

	"github.com/johannaojeling/go-beam-pipeline/pkg/internal/testutils/esutils"
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
		Key string `json:"key"`
	}

	s.T().Run("Should read from Elasticsearch", func(t *testing.T) {
		addresses := []string{s.URL}
		index := "testindex"

		readCfg := ReadConfig{
			Addresses: addresses,
			Index:     index,
			Query:     `{"match_all": {}}`,
			KeepAlive: "1m",
		}
		elemType := reflect.TypeOf(doc{})

		input := []map[string]any{
			{"key": "val1"},
			{"key": "val2"},
		}

		esCfg := elasticsearch.Config{Addresses: addresses}
		client, err := elasticsearch.NewClient(esCfg)
		if err != nil {
			t.Fatalf("failed to initialize client: %v", err)
		}

		ctx := context.Background()
		err = esutils.IndexDocuments(ctx, client, index, input)
		if err != nil {
			t.Fatalf("failed to index documents: %v", err)
		}

		err = esutils.RefreshIndices(ctx, client, []string{index})
		if err != nil {
			t.Fatalf("failed to refresh index: %v", err)
		}

		beam.Init()
		pipeline, scope := beam.NewPipelineWithRoot()

		actual := Read(scope, readCfg, elemType)
		expected := []any{doc{Key: "val1"}, doc{Key: "val2"}}

		passert.Equals(scope, actual, expected...)
		ptest.RunAndValidate(t, pipeline)

		s.TearDownTest(ctx, client, index)
	})
}
