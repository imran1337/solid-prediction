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
	channeltypes "uploadfilesmicroservice/typeDef"

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
	Psf_file_name       string
	Universal_uuid      string
	Parent_Package_Name string
	Version             int
	Vendor              string
	User                string
}

type requestInfo struct {
	id     string
	status string // "Completed", "Not Completed"
}

var requestsQueue []requestInfo

func main() {
	// This needs to be changed when the server goes to production.
	prod := true

	if !prod {
		err := godotenv.Load()
		if err != nil {
			fmt.Printf("Error loading .env file")
		}
	}

	app := fiber.New(fiber.Config{
		BodyLimit: 200 * 1024 * 1024,
	})
	// To allow cross origin, only for local development
	if !prod {
		app.Use(cors.New(cors.Config{
			AllowOrigins: "http://localhost:3000",
			AllowHeaders: "Origin, Content-Type, Accept"}))
	}

	MAX_WORKERS := 10
	MAX_BUFFER := 10

	app.Post("/send/:flag", func(c *fiber.Ctx) error {
		choiceFlag := c.Params("flag")

		// Creates UUID
		id := uuid.New()

		// Receives all the header + file
		fileFromPost, err := c.FormFile("File")
		if err != nil {
			errorFunc.ErrorFormated("E000001")
			return err //c.SendStatus(500)
		}

		fileName := fileFromPost.Filename
		fileNameOnly := strings.TrimSuffix(fileName, filepath.Ext(fileName))
		fmt.Println(fileNameOnly)

		// Check if the file is a zip file
		if filepath.Ext(fileName) != ".zip" {
			errorFunc.ErrorFormated("E000003")
			return err //c.SendStatus(500)
		}

		// Normal flag is the usual branch the file follows, if it's a duplicate the front end will be notified, and a new branch will activate.

		if choiceFlag == "normal" {
			start := time.Now()

			newRequest := requestInfo{
				id:     id.String(),
				status: "Not Completed",
			}
			requestsQueue = append(requestsQueue, newRequest)

			multiPartfile, err := fileFromPost.Open()
			if err != nil {
				errorFunc.ErrorFormated("E000002")
				return err //c.SendStatus(500)
			}

			defer multiPartfile.Close()

			encryptedFile, err := io.ReadAll(multiPartfile)
			if err != nil {
				errorFunc.ErrorFormated("E000004")
				return err //c.SendStatus(500)
			}

			// Key 32 bytes length
			key, _ := os.ReadFile("RandomNumbers")
			block, err := aes.NewCipher(key)
			if err != nil {
				errorFunc.ErrorFormated("E000005")
				return err //c.SendStatus(500)
			}
			// We are going to use the GCM mode, which is a stream mode with authentication.
			// So we don’t have to worry about the padding or doing the authentication, since it is already done by the package.
			gcm, err := cipher.NewGCM(block)
			if err != nil {
				errorFunc.ErrorFormated("E000006")
				return err //c.SendStatus(500)
			}
			// This mode requires a nonce array. It works like an IV.
			// Make sure this is never the same value, that is, change it every time you will encrypt, even if it is the same file.
			// You can do this with a random value, using the package crypto/rand.
			// Never use more than 2^32 random nonces with a given key because of the risk of repeat.
			nonce := make([]byte, gcm.NonceSize())
			if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
				errorFunc.ErrorFormated("E000007")
				return err //c.SendStatus(500)
			}

			// Removing the nonce
			nonce2 := encryptedFile[:gcm.NonceSize()]
			encryptedFile = encryptedFile[gcm.NonceSize():]
			decryptedFile, err := gcm.Open(nil, nonce2, encryptedFile, nil)
			if err != nil {
				errorFunc.ErrorFormated("E000008")
				return err //c.SendStatus(500)
			}
			err = os.WriteFile(fileName, decryptedFile, 0777)
			if err != nil {
				errorFunc.ErrorFormated("E000009")
				return err //c.SendStatus(500)
			}
			file, err := os.Open(fileName)
			if err != nil {
				errorFunc.ErrorFormated("E000010")
				return err //c.SendStatus(500)
			}

			// Unzip to temp folder
			dst := "output/" + id.String()
			archive, err := zip.OpenReader(fileFromPost.Filename)
			if err != nil {
				errorFunc.ErrorFormated("E000011")
				return err //c.SendStatus(500)
			}

			imgFolderExists := false
			psfFolderExists := false

			for _, f := range archive.File {
				if f.FileInfo().Name() == "img" {
					imgFolderExists = true
				}
				if f.FileInfo().Name() == "psf" {
					psfFolderExists = true
				}

			}

			if !imgFolderExists || !psfFolderExists {
				archive.Close()
				file.Close()
				err = os.Remove("./" + fileName)
				if err != nil {
					errorFunc.ErrorFormated("E000012")
					return err //c.SendStatus(500)
				}
				c.SendString("E000013")
				return err //c.SendStatus(500)
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
					errorFunc.ErrorFormated("E000014")
					return err //c.SendStatus(500)
				}

				dstFile, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, f.Mode())
				if err != nil {
					errorFunc.ErrorFormated("E000015")
					return err //c.SendStatus(500)
				}

				fileInArchive, err := f.Open()
				if err != nil {
					errorFunc.ErrorFormated("E000016")
					return err //c.SendStatus(500)
				}

				if _, err := io.Copy(dstFile, fileInArchive); err != nil {
					errorFunc.ErrorFormated("E000017")
					return err //c.SendStatus(500)
				}
				dstFile.Close()
				fileInArchive.Close()

			}
			archive.Close()
			file.Close()
			workingDir := strings.Join([]string{"./output", id.String(), fileNameOnly}, "/")
			removeDir := strings.Join([]string{"./output", id.String()}, "/")

			// Rename img folder file names
			imgBaseDir := workingDir + "/img/"
			imgDir, err := os.ReadDir(imgBaseDir)
			if err != nil {
				errorFunc.ErrorFormated("E000018")
				return err //c.SendStatus(500)
			}
			for i := range imgDir {
				singleImageCurrentDir := imgBaseDir + imgDir[i].Name()
				singleImageNewDir := imgBaseDir + id.String() + imgDir[i].Name()
				err = os.Rename(singleImageCurrentDir, singleImageNewDir)
				if err != nil {
					errorFunc.ErrorFormated("E000019")
					return err //c.SendStatus(500)
				}
			}

			// Rename psf folder file names
			psfBaseDir := workingDir + "/psf/"
			psfDir, err := os.ReadDir(psfBaseDir)
			if err != nil {
				errorFunc.ErrorFormated("E000020")
				return err //c.SendStatus(500)
			}
			for i := range psfDir {
				singlePsfCurrentDir := psfBaseDir + psfDir[i].Name()
				singlePsfNewDir := psfBaseDir + id.String() + psfDir[i].Name()
				err = os.Rename(singlePsfCurrentDir, singlePsfNewDir)
				if err != nil {
					errorFunc.ErrorFormated("E000021")
					return err //c.SendStatus(500)
				}
			}

			// Eliminate the zip file
			err = os.Remove("./" + fileName)
			if err != nil {
				errorFunc.ErrorFormated("E000012")
				return err //c.SendStatus(500)
			}

			JSONDir, err := os.ReadDir(workingDir)
			if err != nil {
				errorFunc.ErrorFormated("E000022")
				return err //c.SendStatus(500)
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
				errorFunc.ErrorFormated("E000023")
				return err //c.SendStatus(500)
			}

			var result []JSONData
			err = json.Unmarshal(byteValue, &result)
			if err != nil {
				errorFunc.ErrorFormated("E000024")
				return err //c.SendStatus(500)
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
				result[iStruct].Psf_file_name = id.String() + result[iStruct].Psf_file_name
				result[iStruct].User = "dmelim@unevis.de"
			}

			// Connect to database
			// Set client options, the string is the connection to the mongo uri
			mongoDBURI := os.Getenv("MONGODB_URI")
			serverAPIOptions := options.ServerAPI(options.ServerAPIVersion1)
			clientOptions := options.Client().ApplyURI(mongoDBURI).SetServerAPIOptions(serverAPIOptions)

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			// Connect to MongoDB
			client, err := mongo.Connect(ctx, clientOptions)
			//client, err := mongo.Connect(context.TODO(), clientOptions)
			if err != nil {
				errorFunc.ErrorFormated("E000025")
				return err //c.SendStatus(500)
			}

			// get collection as ref, the name of the database, then the name of the collection

			collection := client.Database("slim-prediction").Collection("JSONInfo")

			// Check if the files already exist
			filter := bson.D{{Key: "parent_package_name", Value: fileNameOnly}}
			cursor, err := collection.Find(context.TODO(), filter)
			if err != nil {
				if err == mongo.ErrNoDocuments {
					fmt.Println("Clear")
				}
				errorFunc.ErrorFormated(err.Error())
				return err //c.SendStatus(500)
			}
			var results []JSONData
			if err = cursor.All(context.TODO(), &results); err != nil {
				errorFunc.ErrorFormated("E000027")
				return err //c.SendStatus(500)
			}
			if len(results) > 0 {
				err = os.RemoveAll("./output")
				if err != nil {
					errorFunc.ErrorFormated("E000028")
					return err //c.SendStatus(500)
				}
				return c.SendString("File already exists!")
			}

			// Insert many
			newResults := []interface{}{}
			for i := range result {
				newResults = append(newResults, result[i])
			}

			_, err = collection.InsertMany(context.TODO(), newResults)
			if err != nil {
				errorFunc.ErrorFormated("E000029")
				return err //c.SendStatus(500)
			}

			// Initialize workers and chanels
			bucket := os.Getenv("S3_BUCKET")
			pythonServerURL := os.Getenv("PYTHON_SERVER_URL")

			jobs := make(chan channeltypes.ImagePath, MAX_BUFFER)
			resultsCh := make(chan channeltypes.Result, MAX_BUFFER)

			// Iterate through the dir and get files for upload
			for worker := 1; worker <= MAX_WORKERS; worker++ {
				go awsupload.UploadDirToS3(bucket, jobs, resultsCh)

			}
			path := workingDir
			fileList := []string{}

			filepath.Walk(path, func(path string, info os.FileInfo, err error) error {
				if !info.IsDir() && filepath.Ext(info.Name()) != ".json" {
					fileList = append(fileList, path)
				}
				return nil
			})
			totalImageLen := len(fileList)
			var uploadedList []channeltypes.Result

			// Initialize variables and upload images
			go func() {
				for _, pathOfFile := range fileList {
					jobs <- channeltypes.ImagePath{FilePath: pathOfFile}
				}
				close(jobs)
			}()
			go func() error {
				for chItems := range resultsCh {
					uploadedList = append(uploadedList, chItems)
					if totalImageLen == len(uploadedList) {
						elapsed := time.Since(start)
						fmt.Printf("\nRequest took %f", elapsed.Seconds())
						_, err := finishingFuncs.CleanUp(removeDir)
						if err != nil {
							fmt.Println(err)
						}
						_, err = finishingFuncs.ServerCommunication(id.String(), fileNameOnly, pythonServerURL)
						if err != nil {
							fmt.Println(err)
						}

						for i, requests := range requestsQueue {
							if requests.id == id.String() {
								requestsQueue[i].status = "Completed"
							}
						}
					}
				}
				fmt.Println("Completed")
				return nil
			}()
			c.SendString(id.String())
		}
		if choiceFlag == "newVersion" {
			c.SendString("New Version")

		}

		return c.SendStatus(200)
	})

	defaultPort := "5000"

	if !prod {
		defaultPort = "4000"
	}

	port := os.Getenv("PORT")
	if port == "" {
		port = defaultPort
	}

	log.Fatal(app.Listen(":" + port))
}
