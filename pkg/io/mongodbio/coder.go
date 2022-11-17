package mongodbio

import (
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func init() {
	beam.RegisterCoder(reflect.TypeOf((*primitive.ObjectID)(nil)).Elem(), encode, decode)
}

func encode(objectID primitive.ObjectID) []byte {
	return objectID[:]
}

func decode(bytes []byte) primitive.ObjectID {
	var objectID primitive.ObjectID

	copy(objectID[:], bytes[:12])

	return objectID
}
