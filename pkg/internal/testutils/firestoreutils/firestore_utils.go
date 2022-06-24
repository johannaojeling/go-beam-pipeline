package firestoreutils

import (
	"context"
	"fmt"

	"cloud.google.com/go/firestore"
	"google.golang.org/api/iterator"
)

func ReadDocuments(project string, collection string) ([]map[string]interface{}, error) {
	ctx := context.Background()
	client, err := firestore.NewClient(ctx, project)
	if err != nil {
		return nil, fmt.Errorf("error creating Firestore client: %v", err)
	}
	defer client.Close()

	collectionRef := client.Collection(collection)
	iter := collectionRef.Documents(ctx)
	defer iter.Stop()

	records := make([]map[string]interface{}, 0)

	for {
		docSnap, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("error iterating: %v", err)
		}

		var record map[string]interface{}
		err = docSnap.DataTo(&record)
		if err != nil {
			return nil, fmt.Errorf("error parsing document to map: %v", err)
		}

		records = append(records, record)
	}
	return records, nil
}

func WriteDocuments(project string, collection string, records []map[string]interface{}) error {
	ctx := context.Background()
	client, err := firestore.NewClient(ctx, project)
	if err != nil {
		return fmt.Errorf("error creating Firestore client: %v", err)
	}
	defer client.Close()

	batch := client.Batch()
	collectionRef := client.Collection(collection)

	for _, record := range records {
		docRef := collectionRef.NewDoc()
		batch.Create(docRef, record)
	}

	_, err = batch.Commit(ctx)
	if err != nil {
		return fmt.Errorf("error committing batch: %v", err)
	}
	return nil
}
