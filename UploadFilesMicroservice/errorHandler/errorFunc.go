package errorFunc

import (
	"context"
	"fmt"
	mongocode "uploadfilesmicroservice/mongoCode"
	typeDef "uploadfilesmicroservice/typeDef"
)

func ErrorFormated(requestInfo *typeDef.RequestInfo, mongoInfo *typeDef.MongoParts, errCode string, ErrComplete string) (status int) {
	fmt.Println(requestInfo, mongoInfo)
	mongoInfo.MongoCollectionName = "test"
	requestInfo.ErrCode = errCode
	requestInfo.ErrComplete = ErrComplete
	collection, err := mongocode.ConnectToMongoAtlas(mongoInfo)
	// If we get an error on the error func we have no fallback.
	if err != nil {
		fmt.Println(err) //c.SendStatus(500)
	}
	_, err = collection.InsertOne(context.TODO(), requestInfo)
	if err != nil {
		fmt.Println(err) //c.SendStatus(500)
	}
	return 500
}
