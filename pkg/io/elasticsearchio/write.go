package elasticsearchio

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
	"github.com/elastic/go-elasticsearch/v8/esutil"
)

func init() {
	register.DoFn3x1[context.Context, beam.X, func(string), error](&writeFn{})
	register.Emitter1[string]()
}

type WriteConfig struct {
	Addresses  []string
	CloudId    string
	ApiKey     string
	Index      string
	FlushBytes int
}

func Write(
	scope beam.Scope,
	cfg WriteConfig,
	col beam.PCollection,
) {
	scope = scope.Scope("firestoreio.Write")
	elemType := col.Type().Type()
	beam.ParDo(
		scope,
		newWriteFn(cfg, elemType),
		col,
	)
}

type writeFn struct {
	esFn
	FlushBytes  int
	bulkIndexer esutil.BulkIndexer
}

func newWriteFn(
	cfg WriteConfig,
	elemType reflect.Type,
) *writeFn {
	return &writeFn{
		esFn: esFn{
			Addresses: cfg.Addresses,
			CloudId:   cfg.CloudId,
			ApiKey:    cfg.ApiKey,
			Index:     cfg.Index,
			Type:      beam.EncodedType{T: elemType},
		},
		FlushBytes: cfg.FlushBytes,
	}
}

func (fn *writeFn) StartBundle(_ context.Context, _ func(string)) error {
	config := esutil.BulkIndexerConfig{
		Index:      fn.Index,
		Client:     fn.client,
		FlushBytes: fn.FlushBytes,
	}
	bulkIndexer, err := esutil.NewBulkIndexer(config)
	if err != nil {
		return fmt.Errorf("error initializing bulk indexer: %v", err)
	}
	fn.bulkIndexer = bulkIndexer
	return nil
}

func (fn *writeFn) ProcessElement(
	ctx context.Context,
	elem beam.X,
	emit func(string),
) error {
	data, err := json.Marshal(elem)
	if err != nil {
		return fmt.Errorf("error encoding document: %v", err)
	}
	body := bytes.NewReader(data)

	onSuccessFn := func(_ context.Context, _ esutil.BulkIndexerItem, responseItem esutil.BulkIndexerResponseItem) {
		emit(responseItem.DocumentID)
	}

	err = fn.bulkIndexer.Add(
		ctx,
		esutil.BulkIndexerItem{
			Action:    "index",
			Body:      body,
			OnSuccess: onSuccessFn,
			OnFailure: onFailureFn,
		},
	)
	if err != nil {
		return fmt.Errorf("error adding bulk indexer item: %v", err)
	}
	return nil
}

func (fn *writeFn) FinishBundle(ctx context.Context, _ func(string)) error {
	err := fn.bulkIndexer.Close(ctx)
	if err != nil {
		return fmt.Errorf("error closing bulk indexer: %v", err)
	}

	stats := fn.bulkIndexer.Stats()
	if numFailed := stats.NumFailed; numFailed > 0 {
		return fmt.Errorf("error indexing %d document(s)", numFailed)
	}

	fn.bulkIndexer = nil
	return nil
}

func onFailureFn(
	ctx context.Context,
	_ esutil.BulkIndexerItem,
	responseItem esutil.BulkIndexerResponseItem,
	err error,
) {
	if err != nil {
		log.Errorf(ctx, "error indexing document: %v", err)
	} else {
		log.Errorf(
			ctx,
			"error indexing document, type: %v, reason: %v",
			responseItem.Error.Type,
			responseItem.Error.Reason,
		)
	}
}
