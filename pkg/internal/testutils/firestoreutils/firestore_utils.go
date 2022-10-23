package firestoreutils

import (
	"context"
	"fmt"

	"cloud.google.com/go/firestore"
	"google.golang.org/api/iterator"
)

func NewClient(ctx context.Context, project string) (*firestore.Client, error) {
	return firestore.NewClient(ctx, project)
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
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("error iterating: %v", err)
		}

		var record map[string]any
		err = docSnap.DataTo(&record)
		if err != nil {
			return nil, fmt.Errorf("error parsing document to map: %v", err)
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

	_, err := batch.Commit(ctx)
	if err != nil {
		return fmt.Errorf("error committing batch: %v", err)
	}
	return nil
}
