package mongodbio

import (
	"context"
	"fmt"
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

func init() {
	beam.RegisterType(reflect.TypeOf((*mongoDBFn)(nil)))
}

type mongoDBFn struct {
	URL        string
	Database   string
	Collection string
	Type       beam.EncodedType
	client     *mongo.Client
	coll       *mongo.Collection
}

func (fn *mongoDBFn) Setup() error {
	ctx := context.Background()

	client, err := newClient(ctx, fn.URL)
	if err != nil {
		return fmt.Errorf("error initializing MongoDB client: %w", err)
	}

	fn.client = client
	fn.coll = client.Database(fn.Database).Collection(fn.Collection)

	return nil
}

func (fn *mongoDBFn) Teardown() error {
	if err := fn.client.Disconnect(context.Background()); err != nil {
		return fmt.Errorf("error closing MongoDB client: %w", err)
	}

	return nil
}

func newClient(ctx context.Context, url string) (*mongo.Client, error) {
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
