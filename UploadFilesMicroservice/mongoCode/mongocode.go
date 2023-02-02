package mongocode

import (
	"context"
	"time"

	typeDef "uploadfilesmicroservice/typeDef"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func ConnectToMongoAtlas(mongoInfo *typeDef.MongoParts, collName string) (*mongo.Collection, error) {
	// The following code is MongoDB atlas specific
	serverAPIOptions := options.ServerAPI(options.ServerAPIVersion1)
	clientOptions := options.Client().ApplyURI(mongoInfo.MongoURI).SetServerAPIOptions(serverAPIOptions)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Connect to MongoDB
	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		return nil, err //c.SendStatus(500)
	}
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
