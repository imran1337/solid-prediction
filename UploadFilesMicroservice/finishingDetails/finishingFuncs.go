package finishingFuncs

import (
	"bytes"
	"encoding/json"
	"net/http"
	"os"
)

type Message struct {
	UUID     string
	FileName string
}

func CleanUp(dir string) (errLog error) {
	err := os.RemoveAll(dir + "/")
	if err != nil {
		return err
	}
	return nil
}

func ServerCommunication(id string, filename string, receiver string) (message string, errLog error) {
	// Create a new instance of Message
	responseMessage := Message{
		UUID:     id,
		FileName: filename,
	}

	// Marshal it into JSON prior to requesting
	messageJSON, err := json.Marshal(responseMessage)
	if err != nil {
		return "E000033", err
	}

	// Make request with marshalled JSON as the POST body
	_, err = http.Post(receiver, "application/json",
		bytes.NewBuffer(messageJSON))
	if err != nil {
		return "E000034", err
	}
	return "Completed", nil
}
