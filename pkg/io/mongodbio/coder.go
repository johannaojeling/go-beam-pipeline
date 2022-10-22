package mongodbio

import (
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func init() {
	beam.RegisterCoder(reflect.TypeOf((*primitive.ObjectID)(nil)).Elem(), encode, decode)
}

func encode(objectId primitive.ObjectID) []byte {
	return objectId[:]
}

func decode(bytes []byte) primitive.ObjectID {
	var objectId primitive.ObjectID
	copy(objectId[:], bytes[:12])
	return objectId
}
