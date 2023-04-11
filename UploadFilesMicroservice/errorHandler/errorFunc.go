package errorFunc

import (
	"context"
	"log"
	mongocode "uploadfilesmicroservice/mongoCode"
	typeDef "uploadfilesmicroservice/typeDef"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

func ErrorFormated(requestInfo *typeDef.RequestInfo, mongoInfo *typeDef.MongoParts, errCode string, ErrComplete string, client *mongo.Client) (status int) {
	requestInfo.ErrCode = errCode
	requestInfo.ErrComplete = ErrComplete
	requestInfo.Status = "Error"
	collection, err := mongocode.GetMongoCollection(mongoInfo, "requestStatus", client)
	filter := bson.D{{Key: "id", Value: requestInfo.Id}}
	// If we get an error on the error func we have no fallback.
	if err != nil {
		log.Println(err) //c.SendStatus(500)
	}
	_, err = collection.ReplaceOne(context.TODO(), filter, requestInfo)
	if err != nil {
		log.Println(err) //c.SendStatus(500)
	}
	return 500
}
