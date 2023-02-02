package errorFunc

import (
	"context"
	"fmt"
	mongocode "uploadfilesmicroservice/mongoCode"
	typeDef "uploadfilesmicroservice/typeDef"

	"go.mongodb.org/mongo-driver/bson"
)

func ErrorFormated(requestInfo *typeDef.RequestInfo, mongoInfo *typeDef.MongoParts, errCode string, ErrComplete string) (status int) {
	fmt.Println(requestInfo, mongoInfo)
	fmt.Println(mongoInfo)
	requestInfo.ErrCode = errCode
	requestInfo.ErrComplete = ErrComplete
	collection, err := mongocode.ConnectToMongoAtlas(mongoInfo, "requestStatus")
	filter := bson.D{{Key: "id", Value: requestInfo.Id}}
	// If we get an error on the error func we have no fallback.
	if err != nil {
		fmt.Println(err) //c.SendStatus(500)
	}
	_, err = collection.ReplaceOne(context.TODO(), filter, requestInfo)
	if err != nil {
		fmt.Println(err) //c.SendStatus(500)
	}
	return 500
}
