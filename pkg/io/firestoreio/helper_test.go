package firestoreio

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"cloud.google.com/go/firestore"
	"google.golang.org/api/iterator"
)

func NewClient(ctx context.Context, t *testing.T, project string) *firestore.Client {
	t.Helper()

	client, err := firestore.NewClient(ctx, project)
	if err != nil {
		t.Fatalf("error initializing Firestore client: %v", err)
	}

	t.Cleanup(func() {
		client.Close()
	})

	return client
}

func ReadDocuments(
	ctx context.Context,
	t *testing.T,
	client *firestore.Client,
	collection string,
) []map[string]any {
	t.Helper()

	collectionRef := client.Collection(collection)

	iter := collectionRef.Documents(ctx)
	defer iter.Stop()

	var records []map[string]any

	for {
		docSnap, err := iter.Next()
		if errors.Is(err, iterator.Done) {
			break
		}

		if err != nil {
			t.Fatalf("error iterating: %v", err)
		}

		var record map[string]any
		if err := docSnap.DataTo(&record); err != nil {
			t.Fatalf("error parsing document to map: %v", err)
		}

		records = append(records, record)
	}

	return records
}

func WriteDocuments(
	ctx context.Context,
	t *testing.T,
	client *firestore.Client,
	collection string,
	records []map[string]any,
) {
	t.Helper()

	collectionRef := client.Collection(collection)
	batch := client.Batch()

	for _, record := range records {
		docRef := collectionRef.NewDoc()
		batch.Create(docRef, record)
	}

	if _, err := batch.Commit(ctx); err != nil {
		t.Fatalf("error committing batch: %v", err)
	}
}

func FlushDatabase(ctx context.Context, t *testing.T, url string) {
	t.Helper()

	client := &http.Client{}

	request, err := http.NewRequestWithContext(
		ctx,
		http.MethodDelete,
		url,
		http.NoBody,
	)
	if err != nil {
		t.Fatalf("error creating http DELETE request: %v", err)
	}

	response, err := client.Do(request)
	if err != nil {
		t.Fatalf("error performing http DELETE operation: %v", err)
	}

	defer response.Body.Close()

	status := response.Status
	expected := "200 OK"

	if status != expected {
		t.Fatalf("expected status %q but was: %q", expected, status)
	}
}
