package mongocode

import (
	"context"

	typeDef "uploadfilesmicroservice/typeDef"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

func GetMongoCollection(mongoInfo *typeDef.MongoParts, collName string, client *mongo.Client) (*mongo.Collection, error) {
	collection := client.Database(mongoInfo.MongoDBName).Collection(collName)
	return collection, nil
}

func SearchMongo(mongoKey string, mongoValue string, mongoColl *mongo.Collection) (mongoCursor *mongo.Cursor, err error) {
	filter := bson.D{{Key: mongoKey, Value: mongoValue}}
	cursor, err := mongoColl.Find(context.TODO(), filter)
	if err != nil {
		return nil, err
	}
	return cursor, nil
}
