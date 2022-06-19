package elasticsearchio

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	"github.com/elastic/go-elasticsearch/v8/esutil"
)

func init() {
	beam.RegisterType(reflect.TypeOf((*writeFn)(nil)))
}

func Write(
	scope beam.Scope,
	addresses []string,
	cloudId string,
	apiKey string,
	index string,
	flushBytes int,
	col beam.PCollection,
) {
	scope = scope.Scope("firestoreio.Write")
	elemType := col.Type().Type()
	beam.ParDo(
		scope,
		newWriteFn(addresses, cloudId, apiKey, index, flushBytes, elemType),
		col,
	)
}

type writeFn struct {
	esFn
	FlushBytes  int
	bulkIndexer esutil.BulkIndexer
}

func newWriteFn(
	addresses []string,
	cloudId string,
	apiKey string,
	index string,
	flushBytes int,
	elemType reflect.Type,
) *writeFn {
	return &writeFn{
		esFn: esFn{
			Addresses: addresses,
			CloudId:   cloudId,
			ApiKey:    apiKey,
			Index:     index,
			Type:      beam.EncodedType{T: elemType},
		},
		FlushBytes: flushBytes,
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
		return fmt.Errorf("failed to initialize bulk indexer: %v", err)
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
		return fmt.Errorf("failed to encode document: %v", err)
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
		return fmt.Errorf("failed to add bulk indexer item: %v", err)
	}
	return nil
}

func (fn *writeFn) FinishBundle(ctx context.Context, _ func(string)) error {
	err := fn.bulkIndexer.Close(ctx)
	if err != nil {
		return fmt.Errorf("failed to close bulk indexer: %v", err)
	}

	stats := fn.bulkIndexer.Stats()
	if numFailed := stats.NumFailed; numFailed > 0 {
		return fmt.Errorf("failed to index %d document(s)", numFailed)
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
		log.Errorf(ctx, "failed to index document: %v", err)
	} else {
		log.Errorf(
			ctx,
			"failed to index document, type: %v, reason: %v",
			responseItem.Error.Type,
			responseItem.Error.Reason,
		)
	}
}
