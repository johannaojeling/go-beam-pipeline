package firestoreio

import (
	"context"
	"fmt"
	"reflect"

	"cloud.google.com/go/firestore"
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
)

const defaultWriteBatchSize = 500

func init() {
	register.DoFn3x0[context.Context, beam.X, func(string, beam.X)](&createIDFn{})
	register.Emitter2[string, beam.X]()
	register.DoFn4x1[context.Context, string, beam.X, func(string), error](&writeFn{})
	register.Emitter1[string]()
}

type WriteConfig struct {
	Project    string
	Collection string
	BatchSize  int
}

func Write(
	scope beam.Scope,
	cfg WriteConfig,
	col beam.PCollection,
) {
	scope = scope.Scope("firestoreio.Write")
	elemType := col.Type().Type()
	keyed := beam.ParDo(
		scope,
		newCreateIDFn(cfg.Project, cfg.Collection, elemType),
		col,
	)

	shuffled := beam.Reshuffle(scope, keyed)
	shuffledType := shuffled.Type().Type()

	beam.ParDo(
		scope,
		newWriteFn(cfg, shuffledType),
		shuffled,
	)
}

type createIDFn struct {
	firestoreFn
}

func newCreateIDFn(project string, collection string, elemType reflect.Type) *createIDFn {
	return &createIDFn{
		firestoreFn{
			Project:    project,
			Collection: collection,
			Type:       beam.EncodedType{T: elemType},
		},
	}
}

func (fn *createIDFn) ProcessElement(
	_ context.Context,
	elem beam.X,
	emit func(string, beam.X),
) {
	docRef := fn.collectionRef.NewDoc()
	emit(docRef.ID, elem)
}

type writeFn struct {
	firestoreFn
	BatchSize  int
	bulkWriter *firestore.BulkWriter
	jobs       []*firestore.BulkWriterJob
	batchCount int
}

func newWriteFn(cfg WriteConfig, elemType reflect.Type) *writeFn {
	batchSize := cfg.BatchSize
	if batchSize <= 0 {
		batchSize = defaultWriteBatchSize
	}

	return &writeFn{
		firestoreFn: firestoreFn{
			Project:    cfg.Project,
			Collection: cfg.Collection,
			Type:       beam.EncodedType{T: elemType},
		},
		BatchSize: batchSize,
	}
}

func (fn *writeFn) StartBundle(ctx context.Context, _ func(string)) {
	fn.bulkWriter = fn.client.BulkWriter(ctx)
}

func (fn *writeFn) ProcessElement(
	_ context.Context,
	id string,
	elem beam.X,
	emit func(string),
) error {
	docRef := fn.collectionRef.Doc(id)

	job, err := fn.bulkWriter.Set(docRef, &elem)
	if err != nil {
		return fmt.Errorf("error creating document: %w", err)
	}

	fn.jobs = append(fn.jobs, job)

	fn.batchCount++

	if fn.batchCount >= fn.BatchSize {
		if err := fn.flush(); err != nil {
			return fmt.Errorf("error flushing batch: %w", err)
		}
	}

	emit(id)

	return nil
}

func (fn *writeFn) FinishBundle(_ context.Context, _ func(string)) error {
	if fn.batchCount > 0 {
		if err := fn.flush(); err != nil {
			return fmt.Errorf("error flushing batch: %w", err)
		}
	}

	fn.bulkWriter = nil

	return nil
}

func (fn *writeFn) flush() error {
	fn.bulkWriter.Flush()

	if errors := getJobErrors(fn.jobs); errors != nil {
		return fmt.Errorf("error getting job results: %v", errors)
	}

	fn.jobs = nil
	fn.batchCount = 0

	return nil
}

func getJobErrors(jobs []*firestore.BulkWriterJob) []error {
	var errors []error

	for _, job := range jobs {
		if _, err := job.Results(); err != nil {
			errors = append(errors, err)
		}
	}

	return errors
}
