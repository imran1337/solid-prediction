package finishingFuncs

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
)

type Message struct {
	UUID     string
	FileName string
	Vendor   string
}

func CleanUp(dir string) (errLog error) {
	err := os.RemoveAll(dir + "/")
	if err != nil {
		return err
	}
	return nil
}

func ServerCommunication(id string, filename string, receiver string, vendor string) (message string, errLog error) {
	// Create a new instance of Message
	responseMessage := Message{
		UUID:     id,
		FileName: filename,
		Vendor:   vendor,
	}
	fmt.Println(responseMessage)
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
