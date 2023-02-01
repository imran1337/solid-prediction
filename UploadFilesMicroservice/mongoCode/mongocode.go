package mongocode

import (
	"context"
	"time"

	typeDef "uploadfilesmicroservice/typeDef"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func ConnectToMongoAtlas(mongoInfo *typeDef.MongoParts) (*mongo.Collection, error) {
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
	collection := client.Database(mongoInfo.MongoDBName).Collection(mongoInfo.MongoCollectionName)
	return collection, nil
}
