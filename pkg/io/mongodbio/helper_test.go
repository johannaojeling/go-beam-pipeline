package mongodbio

import (
	"context"
	"testing"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

func NewClient(ctx context.Context, t *testing.T, url string) *mongo.Client {
	t.Helper()

	clientOptions := options.Client().ApplyURI(url)

	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		t.Fatalf("error connecting to MongoDB: %v", err)
	}

	t.Cleanup(func() {
		if err := client.Disconnect(ctx); err != nil {
			t.Fatalf("error disconnecting from MongoDB: %v", err)
		}
	})

	readPref := readpref.Primary()
	if err := client.Ping(ctx, readPref); err != nil {
		t.Fatalf("error pinging MongoDB: %v", err)
	}

	return client
}

func DropCollection(ctx context.Context, t *testing.T, collection *mongo.Collection) {
	t.Helper()

	if err := collection.Drop(ctx); err != nil {
		t.Fatalf("error deleting collection %q: %v", collection.Name(), err)
	}
}

func ReadDocuments(
	ctx context.Context,
	t *testing.T,
	collection *mongo.Collection,
) []map[string]any {
	t.Helper()

	cursor, err := collection.Find(ctx, bson.M{})
	if err != nil {
		t.Fatalf("error finding documents: %v", err)
	}

	defer cursor.Close(ctx)

	var documents []map[string]any

	for cursor.Next(ctx) {
		var doc map[string]any
		if err := cursor.Decode(&doc); err != nil {
			t.Fatalf("error decoding document: %v", err)
		}

		delete(doc, "_id")
		documents = append(documents, doc)
	}

	return documents
}

func WriteDocuments(
	ctx context.Context,
	t *testing.T,
	collection *mongo.Collection,
	documents []map[string]any,
) {
	t.Helper()

	docs := make([]any, len(documents))
	for i, doc := range documents {
		docs[i] = doc
	}

	if _, err := collection.InsertMany(ctx, docs); err != nil {
		t.Fatalf("error inserting documents: %v", err)
	}
}
