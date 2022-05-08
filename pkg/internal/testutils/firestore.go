package testutils

import (
	"context"
	"fmt"

	"cloud.google.com/go/firestore"
	"google.golang.org/api/iterator"
)

func ReadDocuments(projectId string, collection string) ([]map[string]interface{}, error) {
	ctx := context.Background()
	client, err := firestore.NewClient(ctx, projectId)
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
