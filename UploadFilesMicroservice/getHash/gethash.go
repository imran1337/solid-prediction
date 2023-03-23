package gethash

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
)

func GetHash(filename string) string {
	fileInfo, err := os.ReadFile(filename)
	if err != nil {
		fmt.Println(err.Error())
	}
	var result map[string]any
	err = json.Unmarshal(fileInfo, &result)
	if err != nil {
		fmt.Println(err.Error())
	}
	// Get params objects
	params := result["parameters"].(map[string]any)
	jsonStr, err := json.Marshal(params)
	if err != nil {
		fmt.Println(err.Error())
	}
	// Convert to hash
	paramsHash := sha256.Sum256(jsonStr)
	paramsHashSlice := paramsHash[:]

	return hex.EncodeToString(paramsHashSlice)
}
