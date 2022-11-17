package mongodbutils

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

func NewClient(ctx context.Context, url string) (*mongo.Client, error) {
	clientOptions := options.Client().ApplyURI(url)

	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		return nil, fmt.Errorf("error connecting to MongoDB: %w", err)
	}

	readPref := readpref.Primary()
	if err := client.Ping(ctx, readPref); err != nil {
		return nil, fmt.Errorf("error pinging MongoDB: %w", err)
	}

	return client, nil
}

func DropCollection(ctx context.Context, collection *mongo.Collection) error {
	if err := collection.Drop(ctx); err != nil {
		return fmt.Errorf("error deleting collection %q: %w", collection.Name(), err)
	}

	return nil
}

func ReadDocuments(ctx context.Context, collection *mongo.Collection) ([]map[string]any, error) {
	cursor, err := collection.Find(ctx, bson.M{})
	if err != nil {
		return nil, fmt.Errorf("error finding documents: %w", err)
	}
	defer cursor.Close(ctx)

	var documents []map[string]any

	for cursor.Next(ctx) {
		var doc map[string]any
		if err := cursor.Decode(&doc); err != nil {
			return nil, fmt.Errorf("error decoding document: %w", err)
		}

		delete(doc, "_id")
		documents = append(documents, doc)
	}

	return documents, nil
}

func WriteDocuments(
	ctx context.Context,
	collection *mongo.Collection,
	documents []map[string]any,
) error {
	docs := make([]any, len(documents))
	for i, doc := range documents {
		docs[i] = doc
	}

	if _, err := collection.InsertMany(ctx, docs); err != nil {
		return fmt.Errorf("error inserting documents: %w", err)
	}

	return nil
}
