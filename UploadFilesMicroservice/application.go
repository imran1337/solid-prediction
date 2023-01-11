package main

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/google/uuid"
	"github.com/joho/godotenv"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type JSONData struct {
	File_name           string
	Vtx_devn_ratio      float32
	Pointcount          int
	Facecount           int
	Vertexcount         int
	Bbox_diagonal       float32
	Polyisland_count    int
	Edge_len_avg        float32
	Material_count      int32
	Material_names      []string
	Image_file_names    []string
	Tri_ratio_median    float32
	Conn_avg            float32
	Min_offset          float32
	Offset_ratio        float32
	Surface_area        float32
	Max_offset          float32
	Border_ratio        float32
	Tri_ratio_avg       float32
	Curvature_avg       float32
	Rating_raw          float32
	Rating              int8
	Psf_file_name       string
	Universal_uuid      string
	Parent_Package_Name string
	Version             int
	Vendor              string
	User                string
}

type DirectoryIterator struct {
	filePaths []string
	bucket    string
	next      struct {
		path string
		f    *os.File
	}
	objType string
	err     error
}

type Message struct {
	UUID     string
	FileName string
}

func NewDirectoryIterator(bucket, dir string, objType string) s3manager.BatchUploadIterator {
	paths := []string{}
	filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		// The directories are not important
		if !info.IsDir() {
			paths = append(paths, path)
		}
		return nil
	})
	return &DirectoryIterator{
		filePaths: paths,
		bucket:    bucket,
		objType:   objType,
	}
}

// Next opens the next file and stops iteration if it fails to open
// a file. Next is a method of NewDirectoryIterator
func (iter *DirectoryIterator) Next() bool {
	if len(iter.filePaths) == 0 {
		iter.next.f = nil
		return false
	}
	f, err := os.Open(iter.filePaths[0])
	iter.err = err

	iter.next.f = f
	iter.next.path = iter.filePaths[0]

	iter.filePaths = iter.filePaths[1:]
	return true && iter.Err() == nil
}

// Err returns an error that was set during opening the file. Err is a method of NewDirectoryIterator.
func (iter *DirectoryIterator) Err() error {
	return iter.err
}

// The key is gonna be the path? Or the name? Need to check

// UploadObject returns a BatchUploadObject and sets the After field to
// close the file. UploadObject is a method of NewDirectoryIterator
func (iter *DirectoryIterator) UploadObject() s3manager.BatchUploadObject {
	f := iter.next.f
	fileName := filepath.Base(f.Name())
	newPath := fmt.Sprintf("/%s/", iter.objType) + fileName
	return s3manager.BatchUploadObject{
		Object: &s3manager.UploadInput{
			Bucket: &iter.bucket,
			Key:    &newPath,
			Body:   f,
		},
		After: func() error {
			return f.Close()
		},
	}
}

func errorFormated(errorMessage string, c *fiber.Ctx) error {
	c.SendString(fmt.Sprintf("You receveid the following error: %s", errorMessage))
	return c.SendStatus(500)
}

func main() {
	prod := false

	if !prod {
		err := godotenv.Load()
		if err != nil {
			fmt.Printf("Error loading .env file")
		}
	}

	app := fiber.New()
	// To allow cross origin, only for local development
	app.Use(cors.New(cors.Config{
		AllowOrigins: "http://localhost:3000",
		AllowHeaders: "Origin, Content-Type, Accept"}))

	app.Post("/send/:flag", func(c *fiber.Ctx) error {
		choiceFlag := c.Params("flag")

		// Creates UUID
		id := uuid.New()

		// Receives all the header + file
		fileFromPost, err := c.FormFile("File")
		if err != nil {
			return errorFormated("E000001", c)
		}

		fileName := fileFromPost.Filename
		fileNameOnly := strings.TrimSuffix(fileName, filepath.Ext(fileName))
		fmt.Println(fileNameOnly)

		// Check if the file is a zip file
		if filepath.Ext(fileName) != ".zip" {
			errorFormated("E000003", c)
		}

		// Normal flag is the usual branch the file follows, if it's a duplicate the front end will be notified, and a new branch will activate.

		if choiceFlag == "normal" {

			multiPartfile, err := fileFromPost.Open()
			if err != nil {
				return errorFormated("E000002", c)
			}

			defer multiPartfile.Close()

			encryptedFile, err := io.ReadAll(multiPartfile)
			if err != nil {
				return errorFormated("E000004", c)
			}

			// Key 32 bytes length
			key, _ := ioutil.ReadFile("RandomNumbers")
			block, err := aes.NewCipher(key)
			if err != nil {
				return errorFormated("E000005", c)
			}
			// We are going to use the GCM mode, which is a stream mode with authentication.
			// So we don’t have to worry about the padding or doing the authentication, since it is already done by the package.
			gcm, err := cipher.NewGCM(block)
			if err != nil {
				return errorFormated("E000006", c)
			}
			// This mode requires a nonce array. It works like an IV.
			// Make sure this is never the same value, that is, change it every time you will encrypt, even if it is the same file.
			// You can do this with a random value, using the package crypto/rand.
			// Never use more than 2^32 random nonces with a given key because of the risk of repeat.
			nonce := make([]byte, gcm.NonceSize())
			if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
				return errorFormated("E000007", c)
			}

			// Removing the nonce
			nonce2 := encryptedFile[:gcm.NonceSize()]
			encryptedFile = encryptedFile[gcm.NonceSize():]
			decryptedFile, err := gcm.Open(nil, nonce2, encryptedFile, nil)
			if err != nil {
				return errorFormated("E000008", c)
			}
			err = ioutil.WriteFile(fileName, decryptedFile, 0777)
			if err != nil {
				return errorFormated("E000009", c)
			}
			file, err := os.Open(fileName)
			if err != nil {
				return errorFormated("E000010", c)
			}

			// Unzip to temp folder?
			dst := "output"
			archive, err := zip.OpenReader(fileFromPost.Filename)
			if err != nil {
				return errorFormated("E000011", c)
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
					return errorFormated("E000012", c)
				}
				c.SendString("E000013")
				return c.SendStatus(500)
			}

			for _, f := range archive.File {
				filePath := filepath.Join(dst, f.Name)
				fmt.Println("unzipping file", filePath)

				if !strings.HasPrefix(filePath, filepath.Clean(dst)+string(os.PathSeparator)) {
					fmt.Println("Invalid File Path")
				}
				if f.FileInfo().IsDir() {
					fmt.Println("Creating directory...")

					os.MkdirAll(filePath, os.ModePerm)
					continue
				}
				if err := os.MkdirAll(filepath.Dir(filePath), os.ModePerm); err != nil {
					return errorFormated("E000014", c)
				}

				dstFile, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, f.Mode())
				if err != nil {
					return errorFormated("E000015", c)
				}

				fileInArchive, err := f.Open()
				if err != nil {
					return errorFormated("E000016", c)
				}

				if _, err := io.Copy(dstFile, fileInArchive); err != nil {
					return errorFormated("E000017", c)
				}
				dstFile.Close()
				fileInArchive.Close()

			}
			archive.Close()
			file.Close()

			// Rename img folder file names
			imgBaseDir := "./output/" + fileNameOnly + "/img/"
			imgDir, err := os.ReadDir(imgBaseDir)
			if err != nil {
				return errorFormated("E000018", c)
			}
			for i := range imgDir {
				singleImageCurrentDir := imgBaseDir + imgDir[i].Name()
				singleImageNewDir := imgBaseDir + id.String() + imgDir[i].Name()
				err = os.Rename(singleImageCurrentDir, singleImageNewDir)
				if err != nil {
					return errorFormated("E000019", c)
				}
			}

			// Rename psf folder file names
			psfBaseDir := "./output/" + fileNameOnly + "/psf/"
			psfDir, err := os.ReadDir(psfBaseDir)
			if err != nil {
				return errorFormated("E000020", c)
			}
			for i := range psfDir {
				singlePsfCurrentDir := psfBaseDir + psfDir[i].Name()
				singlePsfNewDir := psfBaseDir + id.String() + psfDir[i].Name()
				err = os.Rename(singlePsfCurrentDir, singlePsfNewDir)
				if err != nil {
					return errorFormated("E000021", c)
				}
			}

			// Eliminate the zip file
			err = os.Remove("./" + fileName)
			if err != nil {
				return errorFormated("E000012", c)
			}

			JSONDir, err := os.ReadDir("./output/" + fileNameOnly)
			if err != nil {
				return errorFormated("E000022", c)
			}
			var JSONFile string
			for _, JSONDirFile := range JSONDir {
				if filepath.Ext(JSONDirFile.Name()) == ".json" {
					JSONFile = JSONDirFile.Name()
				}

			}
			// Read JSON file, would it be better to just search for the JSON file in the filesystem
			pathToJson := filepath.Join(fileNameOnly, JSONFile)
			byteValue, err := ioutil.ReadFile("./output/" + pathToJson)
			if err != nil {
				return errorFormated("E000023", c)
			}

			var result []JSONData
			err = json.Unmarshal(byteValue, &result)
			if err != nil {
				return errorFormated("E000024", c)
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
			clientOptions := options.Client().ApplyURI(mongoDBURI)

			// Connect to MongoDB

			client, err := mongo.Connect(context.TODO(), clientOptions)
			if err != nil {
				return errorFormated("E000025", c)
			}

			// get collection as ref, the name of the database, then the name of the collection

			collection := client.Database("test").Collection("JSONInfo")

			// Check if the files already exist
			filter := bson.D{{Key: "parent_package_name", Value: fileNameOnly}}
			cursor, err := collection.Find(context.TODO(), filter)
			if err != nil {
				if err == mongo.ErrNoDocuments {
					fmt.Println("Clear")
				}
				return errorFormated("E000026", c)
			}
			var results []JSONData
			if err = cursor.All(context.TODO(), &results); err != nil {
				return errorFormated("E000027", c)
			}
			if len(results) > 0 {
				err = os.RemoveAll("./output")
				if err != nil {
					return errorFormated("E000028", c)
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
				return errorFormated("E000029", c)
			}

			// Initialize a session.

			sess, err := session.NewSessionWithOptions(session.Options{
				Config: aws.Config{
					Region: aws.String("eu-central-1"),
				},
			})
			if err != nil {
				return errorFormated("E000030", c)
			}
			s3Bucket := os.Getenv("S3_BUCKET")

			// Initialize variables and upload images

			path := "./output/" + fileNameOnly + "/img"
			iter := NewDirectoryIterator(s3Bucket, path, "img")
			uploader := s3manager.NewUploader(sess)

			if err := uploader.UploadWithIterator(aws.BackgroundContext(), iter); err != nil {
				return errorFormated("E000031", c)
			}
			fmt.Printf("Successfully uploaded %q to %q", path, s3Bucket)

			// Initialize variables and upload psf

			path = "./output/" + fileNameOnly + "/psf"
			iter = NewDirectoryIterator(s3Bucket, path, "psf")
			uploader = s3manager.NewUploader(sess)

			if err := uploader.UploadWithIterator(aws.BackgroundContext(), iter); err != nil {
				return errorFormated("E000032", c)
			}
			fmt.Printf("Successfully uploaded %q to %q", path, s3Bucket)

			// Delete the created files
			err = os.RemoveAll("./output")
			if err != nil {
				return errorFormated("E000028", c)
			}

			// Create a new instance of Message
			message := Message{
				UUID:     id.String(),
				FileName: fileNameOnly,
			}

			// Marshal it into JSON prior to requesting
			messageJSON, err := json.Marshal(message)
			if err != nil {
				return errorFormated("E000033", c)
			}
			pythonServerURL := os.Getenv("PYTHON_SERVER_URL")

			// Make request with marshalled JSON as the POST body
			_, err = http.Post(pythonServerURL, "application/json",
				bytes.NewBuffer(messageJSON))
			if err != nil {
				return errorFormated("E000034", c)
			}

			return c.SendString("Finished")
		}
		if choiceFlag == "newVersion" {
			c.SendString("New Version")

		}

		return c.SendStatus(500)
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
