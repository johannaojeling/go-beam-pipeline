package firestoreutils

import (
	"context"
	"errors"
	"fmt"

	"cloud.google.com/go/firestore"
	"google.golang.org/api/iterator"
)

func NewClient(ctx context.Context, project string) (*firestore.Client, error) {
	client, err := firestore.NewClient(ctx, project)
	if err != nil {
		return nil, fmt.Errorf("error initializing Firestore client: %w", err)
	}

	return client, nil
}

func ReadDocuments(
	ctx context.Context,
	client *firestore.Client,
	collection string,
) ([]map[string]any, error) {
	collectionRef := client.Collection(collection)

	iter := collectionRef.Documents(ctx)
	defer iter.Stop()

	records := make([]map[string]any, 0)

	for {
		docSnap, err := iter.Next()
		if errors.Is(err, iterator.Done) {
			break
		}

		if err != nil {
			return nil, fmt.Errorf("error iterating: %w", err)
		}

		var record map[string]any
		if err := docSnap.DataTo(&record); err != nil {
			return nil, fmt.Errorf("error parsing document to map: %w", err)
		}

		records = append(records, record)
	}

	return records, nil
}

func WriteDocuments(
	ctx context.Context,
	client *firestore.Client,
	collection string,
	records []map[string]any,
) error {
	collectionRef := client.Collection(collection)
	batch := client.Batch()

	for _, record := range records {
		docRef := collectionRef.NewDoc()
		batch.Create(docRef, record)
	}

	if _, err := batch.Commit(ctx); err != nil {
		return fmt.Errorf("error committing batch: %w", err)
	}

	return nil
}
