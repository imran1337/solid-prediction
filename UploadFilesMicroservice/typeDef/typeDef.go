package typeDef

type ImagePath struct {
	FilePath string
}

type Result struct {
	ReturnMessage string
}

type RequestInfo struct {
	Id          string
	Status      string // "Completed", "Not Completed", "Error"
	ErrComplete string
	ErrCode     string
}

type MongoParts struct {
	MongoURI    string
	MongoDBName string
}
