package main

import (
	"archive/zip"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	awsupload "uploadfilesmicroservice/awsCode"
	errorFunc "uploadfilesmicroservice/errorHandler"
	finishingFuncs "uploadfilesmicroservice/finishingDetails"
	gethash "uploadfilesmicroservice/getHash"
	mongocode "uploadfilesmicroservice/mongoCode"
	typeDef "uploadfilesmicroservice/typeDef"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/google/uuid"
	"github.com/joho/godotenv"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type JSONData struct {
	Category            string
	File_name           string
	Tri_ratio_median    float32
	Rating_0_10         int8
	Conn_avg            float32
	Rating_raw          float32
	Min_offset          float32
	Offset_ratio        float32
	Surface_area        float32
	Vertexcount         int
	Bbox_diagonal       float32
	Edge_len_avg        float32
	Max_offset          float32
	Border_ratio        float32
	Vtx_devn_ratio      float32
	Tri_ratio_avg       float32
	Curvature_avg       float32
	Polyisland_count    int
	Facecount           int
	Pointcount          int
	Material_count      int32
	Material_names      []string
	Image_file_names    []string
	Preset_file_name    string
	Universal_uuid      string
	Parent_Package_Name string
	Version             int
	Vendor              string
	User                string
}

type JSONDataMETA struct {
	Category         string
	File_name        string
	Tri_ratio_median float32
	Rating_0_10      int8
	Conn_avg         float32
	Rating_raw       float32
	Min_offset       float32
	Offset_ratio     float32
	Surface_area     float32
	Vertexcount      int
	Bbox_diagonal    float32
	Edge_len_avg     float32
	Max_offset       float32
	Border_ratio     float32
	Vtx_devn_ratio   float32
	Tri_ratio_avg    float32
	Curvature_avg    float32
	Polyisland_count int
	Facecount        int
	Pointcount       int
	Material_count   int32
	Material_names   []string
	Image_file_names []string
	Psf_file_name    string
}

func main() {
	// This needs to be changed when the server goes to production.
	prod := false

	if !prod {
		err := godotenv.Load()
		if err != nil {
			fmt.Printf("Error loading .env file")
		}
	}
	FRONT_END_URI := os.Getenv("FRONTEND_URL")
	app := fiber.New(fiber.Config{
		BodyLimit: 200 * 1024 * 1024,
	})
	app.Use(cors.New(cors.Config{
		AllowOrigins: FRONT_END_URI,
		AllowHeaders: "Origin, Content-Type, Accept"}))
	// To allow cross origin, only for local development
	if !prod {
		app.Use(cors.New(cors.Config{
			AllowOrigins: "http://localhost:3000",
			AllowHeaders: "Origin, Content-Type, Accept"}))
	}

	MAX_WORKERS := 10
	MAX_BUFFER := 10
	mongoDBURI := os.Getenv("MONGODB_URI")
	mongoDB := os.Getenv("MONGO_DB")
	if !prod {
		mongoDBURI = os.Getenv("MONGO_TEST")
	}
	//Setup AWS session
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String("eu-central-1")},
	)
	if err != nil {
		fmt.Println(err)
	}
	svc := s3.New(sess)
	bucket := os.Getenv("S3_BUCKET")
	if !prod {
		bucket = os.Getenv("S3_TEST_BUCKET")
	}
	// Connect the mongo client
	// The following code is MongoDB atlas specific
	serverAPIOptions := options.ServerAPI(options.ServerAPIVersion1)
	mongoInfo := &typeDef.MongoParts{MongoURI: mongoDBURI, MongoDBName: mongoDB}
	clientOptions := options.Client().ApplyURI(mongoInfo.MongoURI).SetServerAPIOptions(serverAPIOptions).SetMaxPoolSize(5)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Connect to MongoDB
	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		fmt.Println(err.Error()) //c.SendStatus(500)
	}

	app.Get("/", func(c *fiber.Ctx) error { return c.SendStatus(200) })
	app.Get("/test", func(c *fiber.Ctx) error {
		awsupload.SearchAndMatch(svc, bucket, "test")
		return c.SendStatus(200)
	})
	app.Post("/decrypt", func(c *fiber.Ctx) error {
		c.Set("Access-Control-Allow-Origin", "https://www.solidmeta.unevis.de")

		id := uuid.New()

		newRequest := &typeDef.RequestInfo{
			Id:          id.String(),
			Status:      "Running",
			ErrComplete: "none",
			ErrCode:     "none",
		}
		// Connect to database
		// Set client options, the string is the connection to the mongo uri
		collection, err := mongocode.GetMongoCollection(mongoInfo, "requestStatus", client)
		if err != nil {
			c.SendString("Error, ask the admin to check the id:" + id.String())
			return c.SendStatus(errorFunc.ErrorFormated(newRequest, mongoInfo, "E000025", err.Error(), client))
		}
		// Insert the first instance of the request. While it is being processed the user can consult it later.
		_, err = collection.InsertOne(context.TODO(), newRequest)
		if err != nil {
			c.SendString("Error, ask the admin to check the id:" + id.String())
			return c.SendStatus(errorFunc.ErrorFormated(newRequest, mongoInfo, "E000029", err.Error(), client))
		}
		fileFromPost, err := c.FormFile("File")
		if err != nil {
			c.SendString("Error, ask the admin to check the id:" + id.String())
			return c.SendStatus(errorFunc.ErrorFormated(newRequest, mongoInfo, "E000001", err.Error(), client))
		}
		fileName := fileFromPost.Filename
		fmt.Println(fileName)
		fileNameOnly := strings.TrimSuffix(fileName, filepath.Ext(fileName))
		multiPartfile, err := fileFromPost.Open()
		if err != nil {
			c.SendString("Error, ask the admin to check the id:" + id.String())

			return c.SendStatus(errorFunc.ErrorFormated(newRequest, mongoInfo, "E000002", err.Error(), client))
		}

		defer multiPartfile.Close()

		encryptedFile, err := io.ReadAll(multiPartfile)
		if err != nil {
			c.SendString("Error, ask the admin to check the id:" + id.String())

			return c.SendStatus(errorFunc.ErrorFormated(newRequest, mongoInfo, "E000004", err.Error(), client))
		}

		// Key 32 bytes length
		key := []byte(os.Getenv("ENC_KEY"))

		//key, _ := os.ReadFile("RandomNumbers")
		block, err := aes.NewCipher(key)
		if err != nil {
			c.SendString("Error, ask the admin to check the id:" + id.String())

			return c.SendStatus(errorFunc.ErrorFormated(newRequest, mongoInfo, "E000005", err.Error(), client))
		}
		// We are going to use the GCM mode, which is a stream mode with authentication.
		// So we don’t have to worry about the padding or doing the authentication, since it is already done by the package.
		gcm, err := cipher.NewGCM(block)
		if err != nil {
			c.SendString("Error, ask the admin to check the id:" + id.String())

			return c.SendStatus(errorFunc.ErrorFormated(newRequest, mongoInfo, "E000006", err.Error(), client))
		}
		// This mode requires a nonce array. It works like an IV.
		// Make sure this is never the same value, that is, change it every time you will encrypt, even if it is the same file.
		// You can do this with a random value, using the package crypto/rand.
		// Never use more than 2^32 random nonces with a given key because of the risk of repeat.
		nonce := make([]byte, gcm.NonceSize())
		if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
			c.SendString("Error, ask the admin to check the id:" + id.String())

			return c.SendStatus(errorFunc.ErrorFormated(newRequest, mongoInfo, "E000007", err.Error(), client))
		}

		// Removing the nonce
		nonce2 := encryptedFile[:gcm.NonceSize()]
		encryptedFile = encryptedFile[gcm.NonceSize():]
		decryptedFile, err := gcm.Open(nil, nonce2, encryptedFile, nil)
		if err != nil {
			c.SendString("Error, ask the admin to check the id:" + id.String())

			return c.SendStatus(errorFunc.ErrorFormated(newRequest, mongoInfo, "E000008", err.Error(), client))
		}
		fileName = fileNameOnly + ".zip"
		err = os.WriteFile(fileName, decryptedFile, 0777)
		if err != nil {
			c.SendString("Error, ask the admin to check the id:" + id.String())
			return c.SendStatus(errorFunc.ErrorFormated(newRequest, mongoInfo, "E000009", err.Error(), client))
		}
		file, err := os.Open(fileName)
		if err != nil {
			c.SendString("Error, ask the admin to check the id:" + id.String())
			return c.SendStatus(errorFunc.ErrorFormated(newRequest, mongoInfo, "E000010", err.Error(), client))
		}

		// Unzip to temp folder
		dst := "decrypt/" + id.String()
		archive, err := zip.OpenReader(fileName)
		if err != nil {
			c.SendString("Error, ask the admin to check the id:" + id.String())
			return c.SendStatus(errorFunc.ErrorFormated(newRequest, mongoInfo, "E000011", err.Error(), client))
		}
		var jsonFileName string
		for _, f := range archive.File {
			filePath := filepath.Join(dst, f.Name)
			if !strings.HasPrefix(filePath, filepath.Clean(dst)+string(os.PathSeparator)) {
				fmt.Println("Invalid File Path")
			}
			if filepath.Ext(f.Name) == ".json" {
				jsonFileName = f.Name
				if err := os.MkdirAll(filepath.Dir(filePath), os.ModePerm); err != nil {
					c.SendString("Error, ask the admin to check the id:" + id.String())
					return c.SendStatus(errorFunc.ErrorFormated(newRequest, mongoInfo, "E000014", err.Error(), client))
				}

				dstFile, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, f.Mode())
				if err != nil {
					c.SendString("Error, ask the admin to check the id:" + id.String())
					return c.SendStatus(errorFunc.ErrorFormated(newRequest, mongoInfo, "E000015", err.Error(), client))
				}

				fileInArchive, err := f.Open()
				if err != nil {
					c.SendString("Error, ask the admin to check the id:" + id.String())
					return c.SendStatus(errorFunc.ErrorFormated(newRequest, mongoInfo, "E000016", err.Error(), client))
				}

				if _, err := io.Copy(dstFile, fileInArchive); err != nil {
					c.SendString("Error, ask the admin to check the id:" + id.String())

					return c.SendStatus(errorFunc.ErrorFormated(newRequest, mongoInfo, "E000017", err.Error(), client))
				}
				dstFile.Close()
				fileInArchive.Close()
				break
			}

		}
		archive.Close()
		file.Close()
		err = os.RemoveAll("./" + fileNameOnly + ".zip")
		if err != nil {
			print(err.Error())
		}
		fileInfo, err := os.ReadFile(dst + "/" + jsonFileName)
		if err != nil {
			fmt.Println(err.Error())
		}
		var result []JSONDataMETA
		err = json.Unmarshal(fileInfo, &result)
		if err != nil {
			c.SendString("Error, ask the admin to check the id:" + id.String())
			return c.SendStatus(errorFunc.ErrorFormated(newRequest, mongoInfo, "E000024", err.Error(), client))
		}
		defer finishingFuncs.CleanUp("./decrypt/" + id.String())
		return c.JSON(result)
	})
	app.Post("/send/:flag", func(c *fiber.Ctx) error {
		choiceFlag := c.Params("flag")

		// Creates UUID
		id := uuid.New()

		// Set Mongo and the request struct

		mongoInfo := &typeDef.MongoParts{MongoURI: mongoDBURI, MongoDBName: mongoDB}

		newRequest := &typeDef.RequestInfo{
			Id:          id.String(),
			Status:      "Running",
			ErrComplete: "none",
			ErrCode:     "none",
		}

		// Connect to database
		// Set client options, the string is the connection to the mongo uri
		collectionrequestStatus, err := mongocode.GetMongoCollection(mongoInfo, "requestStatus", client)
		if err != nil {
			c.SendString("Error, ask the admin to check the id:" + id.String())
			return c.SendStatus(errorFunc.ErrorFormated(newRequest, mongoInfo, "E000025", err.Error(), client))
		}
		// Insert the first instance of the request. While it is being processed the user can consult it later.
		_, err = collectionrequestStatus.InsertOne(context.TODO(), newRequest)
		if err != nil {
			c.SendString("Error, ask the admin to check the id:" + id.String())
			return c.SendStatus(errorFunc.ErrorFormated(newRequest, mongoInfo, "E000029", err.Error(), client))
		}
		// Receives all the header + file
		fileFromPost, err := c.FormFile("File")
		if err != nil {
			c.SendString("Error, ask the admin to check the id:" + id.String())
			return c.SendStatus(errorFunc.ErrorFormated(newRequest, mongoInfo, "E000001", err.Error(), client))
		}

		fileName := fileFromPost.Filename
		fileNameOnly := strings.TrimSuffix(fileName, filepath.Ext(fileName))
		fmt.Println(fileNameOnly)

		// Check if the file is a zip file
		if filepath.Ext(fileName) != ".smp" {
			c.SendString("Error, ask the admin to check the id:" + id.String())
			return c.SendStatus(errorFunc.ErrorFormated(newRequest, mongoInfo, "E000003", "Invalid Extension", client))
		}

		// Normal flag is the usual branch the file follows, if it's a duplicate the front end will be notified, and a new branch will activate.

		if choiceFlag == "normal" {
			start := time.Now()

			multiPartfile, err := fileFromPost.Open()
			if err != nil {
				c.SendString("Error, ask the admin to check the id:" + id.String())

				return c.SendStatus(errorFunc.ErrorFormated(newRequest, mongoInfo, "E000002", err.Error(), client))
			}

			defer multiPartfile.Close()

			encryptedFile, err := io.ReadAll(multiPartfile)
			if err != nil {
				c.SendString("Error, ask the admin to check the id:" + id.String())

				return c.SendStatus(errorFunc.ErrorFormated(newRequest, mongoInfo, "E000004", err.Error(), client))
			}

			// Key 32 bytes length
			key := []byte(os.Getenv("ENC_KEY"))

			//key, _ := os.ReadFile("RandomNumbers")
			block, err := aes.NewCipher(key)
			if err != nil {
				c.SendString("Error, ask the admin to check the id:" + id.String())

				return c.SendStatus(errorFunc.ErrorFormated(newRequest, mongoInfo, "E000005", err.Error(), client))
			}
			// We are going to use the GCM mode, which is a stream mode with authentication.
			// So we don’t have to worry about the padding or doing the authentication, since it is already done by the package.
			gcm, err := cipher.NewGCM(block)
			if err != nil {
				c.SendString("Error, ask the admin to check the id:" + id.String())

				return c.SendStatus(errorFunc.ErrorFormated(newRequest, mongoInfo, "E000006", err.Error(), client))
			}
			// This mode requires a nonce array. It works like an IV.
			// Make sure this is never the same value, that is, change it every time you will encrypt, even if it is the same file.
			// You can do this with a random value, using the package crypto/rand.
			// Never use more than 2^32 random nonces with a given key because of the risk of repeat.
			nonce := make([]byte, gcm.NonceSize())
			if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
				c.SendString("Error, ask the admin to check the id:" + id.String())

				return c.SendStatus(errorFunc.ErrorFormated(newRequest, mongoInfo, "E000007", err.Error(), client))
			}

			// Removing the nonce
			nonce2 := encryptedFile[:gcm.NonceSize()]
			encryptedFile = encryptedFile[gcm.NonceSize():]
			decryptedFile, err := gcm.Open(nil, nonce2, encryptedFile, nil)
			if err != nil {
				c.SendString("Error, ask the admin to check the id:" + id.String())

				return c.SendStatus(errorFunc.ErrorFormated(newRequest, mongoInfo, "E000008", err.Error(), client))
			}
			fileName = fileNameOnly + ".zip"
			err = os.WriteFile(fileName, decryptedFile, 0777)
			if err != nil {
				c.SendString("Error, ask the admin to check the id:" + id.String())

				return c.SendStatus(errorFunc.ErrorFormated(newRequest, mongoInfo, "E000009", err.Error(), client))
			}
			file, err := os.Open(fileName)
			if err != nil {
				c.SendString("Error, ask the admin to check the id:" + id.String())

				return c.SendStatus(errorFunc.ErrorFormated(newRequest, mongoInfo, "E000010", err.Error(), client))
			}

			// Unzip to temp folder
			dst := "output/" + id.String()
			archive, err := zip.OpenReader(fileName)
			if err != nil {
				c.SendString("Error, ask the admin to check the id:" + id.String())
				return c.SendStatus(errorFunc.ErrorFormated(newRequest, mongoInfo, "E000011", err.Error(), client))
			}

			imgFolderExists := false
			presetFolderExists := false

			for _, f := range archive.File {
				fname := f.FileInfo().Name()
				if filepath.Ext(fname) == ".png" {
					imgFolderExists = true
				}
				if filepath.Ext(fname) == ".preset" {
					presetFolderExists = true
				}

			}
			fmt.Println(imgFolderExists, presetFolderExists)
			if !imgFolderExists || !presetFolderExists {
				archive.Close()
				file.Close()
				err = os.Remove("./" + fileName)
				if err != nil {
					c.SendString("Error, ask the admin to check the id:" + id.String())

					return c.SendStatus(errorFunc.ErrorFormated(newRequest, mongoInfo, "E000012", err.Error(), client))
				}
				c.SendString("Error, ask the admin to check the id:" + id.String())
				return c.SendStatus(errorFunc.ErrorFormated(newRequest, mongoInfo, "E000013", "Error while removing folder.", client))
			}

			for _, f := range archive.File {
				filePath := filepath.Join(dst, f.Name)
				if !strings.HasPrefix(filePath, filepath.Clean(dst)+string(os.PathSeparator)) {
					fmt.Println("Invalid File Path")
				}
				if f.FileInfo().IsDir() {
					os.MkdirAll(filePath, os.ModePerm)
					continue
				}
				if err := os.MkdirAll(filepath.Dir(filePath), os.ModePerm); err != nil {
					c.SendString("Error, ask the admin to check the id:" + id.String())
					return c.SendStatus(errorFunc.ErrorFormated(newRequest, mongoInfo, "E000014", err.Error(), client))
				}

				dstFile, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, f.Mode())
				if err != nil {
					c.SendString("Error, ask the admin to check the id:" + id.String())
					return c.SendStatus(errorFunc.ErrorFormated(newRequest, mongoInfo, "E000015", err.Error(), client))
				}

				fileInArchive, err := f.Open()
				if err != nil {
					c.SendString("Error, ask the admin to check the id:" + id.String())
					return c.SendStatus(errorFunc.ErrorFormated(newRequest, mongoInfo, "E000016", err.Error(), client))
				}

				if _, err := io.Copy(dstFile, fileInArchive); err != nil {
					c.SendString("Error, ask the admin to check the id:" + id.String())

					return c.SendStatus(errorFunc.ErrorFormated(newRequest, mongoInfo, "E000017", err.Error(), client))
				}
				dstFile.Close()
				fileInArchive.Close()

			}
			archive.Close()
			file.Close()

			workingDir := strings.Join([]string{"./output", id.String()}, "/")
			removeDir := strings.Join([]string{"./output", id.String()}, "/")

			// Rename img folder file names
			imgBaseDir := workingDir + "/img/"
			imgDir, err := os.ReadDir(imgBaseDir)
			if err != nil {
				c.SendString("Error, ask the admin to check the id:" + id.String())
				return c.SendStatus(errorFunc.ErrorFormated(newRequest, mongoInfo, "E000018", err.Error(), client))
			}
			for i := range imgDir {
				singleImageCurrentDir := imgBaseDir + imgDir[i].Name()
				singleImageNewDir := imgBaseDir + id.String() + imgDir[i].Name()
				err = os.Rename(singleImageCurrentDir, singleImageNewDir)
				if err != nil {
					c.SendString("Error, ask the admin to check the id:" + id.String())

					return c.SendStatus(errorFunc.ErrorFormated(newRequest, mongoInfo, "E000019", err.Error(), client))
				}
			}
			// Store the original name and the new hash name
			mapHashesNames := make(map[string]string)
			// Rename preset folder file names
			presetBaseDir := workingDir + "/preset/"
			presetDir, err := os.ReadDir(presetBaseDir)
			if err != nil {
				c.SendString("Error, ask the admin to check the id:" + id.String())

				return c.SendStatus(errorFunc.ErrorFormated(newRequest, mongoInfo, "E000020", err.Error(), client))
			}
			for i := range presetDir {

				singlePresetCurrentDir := presetBaseDir + presetDir[i].Name()
				fileHashValue := gethash.GetHash(singlePresetCurrentDir)
				singlePresetNewDir := presetBaseDir + fileHashValue + ".preset"
				mapHashesNames[presetDir[i].Name()] = fileHashValue
				err = os.Rename(singlePresetCurrentDir, singlePresetNewDir)
				if err != nil {
					c.SendString("Error, ask the admin to check the id:" + id.String())

					return c.SendStatus(errorFunc.ErrorFormated(newRequest, mongoInfo, "E000021", err.Error(), client))
				}
			}

			// Eliminate the zip file
			err = os.Remove("./" + fileName)
			if err != nil {
				c.SendString("Error, ask the admin to check the id:" + id.String())

				return c.SendStatus(errorFunc.ErrorFormated(newRequest, mongoInfo, "E000012", err.Error(), client))
			}

			JSONDir, err := os.ReadDir(workingDir)
			if err != nil {
				c.SendString("Error, ask the admin to check the id:" + id.String())

				return c.SendStatus(errorFunc.ErrorFormated(newRequest, mongoInfo, "E000022", err.Error(), client))
			}
			var JSONFile string
			for _, JSONDirFile := range JSONDir {
				if filepath.Ext(JSONDirFile.Name()) == ".json" {
					JSONFile = JSONDirFile.Name()
				}

			}
			// Read JSON file, would it be better to just search for the JSON file in the filesystem
			pathToJson := filepath.Join(workingDir, JSONFile)
			byteValue, err := os.ReadFile(pathToJson)
			if err != nil {
				c.SendString("Error, ask the admin to check the id:" + id.String())

				return c.SendStatus(errorFunc.ErrorFormated(newRequest, mongoInfo, "E000023", err.Error(), client))
			}

			var result []JSONData
			err = json.Unmarshal(byteValue, &result)
			if err != nil {
				c.SendString("Error, ask the admin to check the id:" + id.String())
				return c.SendStatus(errorFunc.ErrorFormated(newRequest, mongoInfo, "E000024", err.Error(), client))
			}

			// Updates certain properties of the JSON file
			for iStruct := range result {
				result[iStruct].Universal_uuid = id.String()
				result[iStruct].Parent_Package_Name = fileNameOnly
				result[iStruct].Version = 1
				result[iStruct].Vendor = "Volkswagen"
				for iImages := range result[iStruct].Image_file_names {
					result[iStruct].Image_file_names[iImages] = id.String() + result[iStruct].Image_file_names[iImages]
				}
				result[iStruct].Preset_file_name = mapHashesNames[result[iStruct].Preset_file_name] + ".preset"
				result[iStruct].User = "dmelim@unevis.de"
			}
			collectionJSONInfo, err := mongocode.GetMongoCollection(mongoInfo, "JSONInfo", client)

			if err != nil {
				c.SendString("Error, ask the admin to check the id:" + id.String())
				return c.SendStatus(errorFunc.ErrorFormated(newRequest, mongoInfo, "E000025", err.Error(), client))
			}
			// Check if the files already exist
			cursor, err := mongocode.SearchMongo("parent_package_name", fileNameOnly, collectionJSONInfo)
			if err != nil {
				if err == mongo.ErrNoDocuments {
					fmt.Println("Clear")
				}
				c.SendString("Error, ask the admin to check the id:" + id.String())
				return c.SendStatus(errorFunc.ErrorFormated(newRequest, mongoInfo, "E000026", err.Error(), client))
			}
			var results []JSONData
			if err = cursor.All(context.TODO(), &results); err != nil {
				c.SendString("Error, ask the admin to check the id:" + id.String())
				return c.SendStatus(errorFunc.ErrorFormated(newRequest, mongoInfo, "E000027", err.Error(), client))
			}
			if len(results) > 0 {
				finishingFuncs.CleanUp(removeDir)
				if err != nil {
					c.SendString("Error, ask the admin to check the id:" + id.String())
					return c.SendStatus(errorFunc.ErrorFormated(newRequest, mongoInfo, "E000028", err.Error(), client))
				}
				return c.SendString("File already exists!")
			}

			// Insert many
			newResults := []interface{}{}
			for i := range result {
				newResults = append(newResults, result[i])
			}

			_, err = collectionJSONInfo.InsertMany(context.TODO(), newResults)
			if err != nil {
				c.SendString("Error, ask the admin to check the id:" + id.String())
				return c.SendStatus(errorFunc.ErrorFormated(newRequest, mongoInfo, "E000029", err.Error(), client))
			}

			// Initialize workers and chanels

			pythonServerURL := os.Getenv("PYTHON_SERVER_URL")

			jobs := make(chan typeDef.ImagePath, MAX_BUFFER)
			resultsCh := make(chan typeDef.Result, MAX_BUFFER)

			// Iterate through the dir and get files for upload
			for worker := 1; worker <= MAX_WORKERS; worker++ {
				go awsupload.UploadDirToS3(bucket, jobs, resultsCh, sess, svc)
			}
			path := workingDir
			fileList := []string{}

			filepath.Walk(path, func(path string, info os.FileInfo, err error) error {
				if !info.IsDir() && filepath.Ext(info.Name()) != ".json" && filepath.Ext(info.Name()) != ".csv" {
					fileList = append(fileList, path)
				}
				return nil
			})
			totalImageLen := len(fileList)
			var uploadedList []typeDef.Result

			// Initialize variables and upload images
			go func() {
				for _, pathOfFile := range fileList {
					jobs <- typeDef.ImagePath{FilePath: pathOfFile}
				}
				close(jobs)
			}()
			go func() int {
				defer finishingFuncs.CleanUp(removeDir)
				for chItems := range resultsCh {
					uploadedList = append(uploadedList, chItems)
					if totalImageLen == len(uploadedList) {
						elapsed := time.Since(start)
						fmt.Printf("\nRequest took %f", elapsed.Seconds())

						filter := bson.D{{Key: "id", Value: newRequest.Id}}
						if err != nil {
							return errorFunc.ErrorFormated(newRequest, mongoInfo, "E000026", err.Error(), client)
						}
						newRequest.Status = "Completed"
						_, err = collectionrequestStatus.ReplaceOne(context.TODO(), filter, newRequest)
						if err != nil {
							return errorFunc.ErrorFormated(newRequest, mongoInfo, "E000029", err.Error(), client)
						}
						errMsg, err := finishingFuncs.ServerCommunication(id.String(), fileNameOnly, pythonServerURL)
						if err != nil {
							fmt.Println("Failed to communicate with the python server")
							return errorFunc.ErrorFormated(newRequest, mongoInfo, errMsg, err.Error(), client)
						}

					}
				}
				fmt.Println("Completed" + id.String())
				return 200
			}()

			c.SendString(id.String())
		}
		if choiceFlag == "newVersion" {
			c.SendString("New Version")

		}

		return c.SendStatus(200)
	})
	app.Get("/:id", func(c *fiber.Ctx) error {
		mongoInfo := &typeDef.MongoParts{MongoURI: mongoDBURI, MongoDBName: mongoDB}
		requestId := c.Params("id")
		collection, err := mongocode.GetMongoCollection(mongoInfo, "requestStatus", client)
		if err != nil {
			fmt.Println(err)
			c.SendString("We encoutered the error E000025, please speak with the server administrator.")
			return c.SendStatus(500)
		}
		cursor, err := mongocode.SearchMongo("id", requestId, collection)
		if err != nil {
			if err == mongo.ErrNoDocuments {
				c.SendString("We didn't find your ID.")
				return c.SendStatus(500)
			}
			fmt.Println(err)
			c.SendString("We encoutered the error E000026, please speak with the server administrator.")
			return c.SendStatus(500)
		}
		var results []typeDef.RequestInfo
		if err = cursor.All(context.TODO(), &results); err != nil {
			c.SendString("We encoutered the error E000027, please speak with the server administrator.")
			return c.SendStatus(500)
		}
		if len(results) == 0 {
			c.SendString("We didn't find your ID.")
			return c.SendStatus(500)
		}
		if results[0].Status == "Error" {
			return c.SendString(fmt.Sprintf("The id: %s, is %s, with the error code: %s", results[0].Id, results[0].Status, results[0].ErrCode))
		}

		return c.SendString(fmt.Sprintf("The id: %s, is %s.", results[0].Id, results[0].Status))
	})

	defaultPort := ":5000"

	if !prod {
		defaultPort = "localhost:4000"
	}

	port := os.Getenv("PORT")
	if port == "" {
		port = defaultPort
	}

	log.Fatal(app.Listen(port))
}
