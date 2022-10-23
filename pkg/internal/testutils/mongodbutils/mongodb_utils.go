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
		return nil, fmt.Errorf("error connecting to MongoDB: %v", err)
	}

	readPref := readpref.Primary()
	err = client.Ping(ctx, readPref)
	if err != nil {
		return nil, fmt.Errorf("error pinging MongoDB: %v", err)
	}
	return client, nil
}

func DropCollection(ctx context.Context, collection *mongo.Collection) error {
	err := collection.Drop(ctx)
	if err != nil {
		return fmt.Errorf("error deleting collection %q: %v", collection.Name(), err)
	}
	return nil
}

func ReadDocuments(ctx context.Context, collection *mongo.Collection) ([]map[string]any, error) {
	cursor, err := collection.Find(ctx, bson.M{})
	if err != nil {
		return nil, fmt.Errorf("error finding documents: %v", err)
	}
	defer cursor.Close(ctx)

	var documents []map[string]any
	for cursor.Next(ctx) {
		var doc map[string]any
		err := cursor.Decode(&doc)
		if err != nil {
			return nil, fmt.Errorf("error decoding document: %v", err)
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
	var docs []any
	for _, doc := range documents {
		docs = append(docs, doc)
	}

	_, err := collection.InsertMany(ctx, docs)
	if err != nil {
		return fmt.Errorf("error inserting documents: %v", err)
	}
	return nil
}
