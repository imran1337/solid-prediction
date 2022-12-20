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
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type JSONData struct {
	File_name        string
	Vtx_devn_ratio   float32
	Pointcount       int
	Facecount        int
	Vertexcount      int
	Bbox_diagonal    float32
	Polyisland_count int
	Edge_len_avg     float32
	Material_count   int32
	Material_names   []string
	Image_file_names []string
	Tri_ratio_median float32
	Conn_avg         float32
	Min_offset       float32
	Offset_ratio     float32
	Surface_area     float32
	Max_offset       float32
	Border_ratio     float32
	Tri_ratio_avg    float32
	Curvature_avg    float32
	Rating_raw       float32
	Rating           int8
}

type DirectoryIterator struct {
	filePaths []string
	bucket    string
	next      struct {
		path string
		f    *os.File
	}
	err error
}

func NewDirectoryIterator(bucket, dir string) s3manager.BatchUploadIterator {
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
	fmt.Println(f.Name())
	fileName := filepath.Base(f.Name())
	dirList := strings.Split(f.Name(), "\\")
	newPath := "/" + dirList[1] + "/" + fileName
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

func errorFormated(errorMessage error, c *fiber.Ctx) error {
	c.SendString(fmt.Sprintf("You receveid the following error: %s", errorMessage.Error()))
	return c.SendStatus(500)
}

func main() {
	app := fiber.New()
	// To allow cross origin
	app.Use(cors.New(cors.Config{
		AllowOrigins: "http://localhost:3000",
		AllowHeaders: "Origin, Content-Type, Accept"}))

	app.Post("/send", func(c *fiber.Ctx) error {
		// Receives all the header + file
		fileFromPost, err := c.FormFile("File")
		if err != nil {
			return errorFormated(err, c)
		}

		fileName := fileFromPost.Filename
		fileNameOnly := strings.TrimSuffix(fileName, filepath.Ext(fileName))
		fmt.Println(fileNameOnly)
		// Check if the file is a zip file
		if filepath.Ext(fileName) != ".zip" {
			c.SendString("Wrong file format!")
			return c.SendStatus(500)

		}

		multiPartfile, err := fileFromPost.Open()
		if err != nil {
			return errorFormated(err, c)
		}

		defer multiPartfile.Close()

		encryptedFile, err := ioutil.ReadAll(multiPartfile)
		if err != nil {
			return errorFormated(err, c)
		}

		// Key 32 bytes length
		key, _ := ioutil.ReadFile("RandomNumbers")
		block, err := aes.NewCipher(key)
		if err != nil {
			return errorFormated(err, c)
		}
		// We are going to use the GCM mode, which is a stream mode with authentication.
		// So we don’t have to worry about the padding or doing the authentication, since it is already done by the package.
		gcm, err := cipher.NewGCM(block)
		if err != nil {
			return errorFormated(err, c)
		}
		// This mode requires a nonce array. It works like an IV.
		// Make sure this is never the same value, that is, change it every time you will encrypt, even if it is the same file.
		// You can do this with a random value, using the package crypto/rand.
		// Never use more than 2^32 random nonces with a given key because of the risk of repeat.
		nonce := make([]byte, gcm.NonceSize())
		if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
			return errorFormated(err, c)
		}

		// Removing the nonce
		nonce2 := encryptedFile[:gcm.NonceSize()]
		encryptedFile = encryptedFile[gcm.NonceSize():]
		decryptedFile, err := gcm.Open(nil, nonce2, encryptedFile, nil)
		if err != nil {
			return errorFormated(err, c)
		}
		err = ioutil.WriteFile(fileName, decryptedFile, 0777)
		if err != nil {
			return errorFormated(err, c)
		}
		file, err := os.Open(fileName)
		if err != nil {
			return errorFormated(err, c)
		}

		// Unzip to temp folder?
		dst := "output"
		archive, err := zip.OpenReader(fileFromPost.Filename)
		if err != nil {
			return errorFormated(err, c)
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
				panic(err)
			}

			dstFile, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, f.Mode())
			if err != nil {
				return errorFormated(err, c)
			}

			fileInArchive, err := f.Open()
			if err != nil {
				return errorFormated(err, c)
			}

			if _, err := io.Copy(dstFile, fileInArchive); err != nil {
				return errorFormated(err, c)
			}
			dstFile.Close()
			fileInArchive.Close()

		}
		archive.Close()
		file.Close()
		// Eliminate the zip file
		err = os.Remove("./" + fileName)
		if err != nil {
			return errorFormated(err, c)
		}

		// Read JSON file
		byteValue, err := ioutil.ReadFile("./output/" + fileNameOnly + "/info.json")
		if err != nil {
			return errorFormated(err, c)
		}

		var result []JSONData
		err = json.Unmarshal(byteValue, &result)
		if err != nil {
			return errorFormated(err, c)
		}

		// Connect to database
		// Set client options, the string is the connection to the mongo uri
		clientOptions := options.Client().ApplyURI("mongodb://mongo:3jrKeUXDbHJ97Rlo9Jor@containers-us-west-67.railway.app:6010")

		// Connect to MongoDB
		client, err := mongo.Connect(context.TODO(), clientOptions)
		if err != nil {
			return errorFormated(err, c)
		}

		// get collection as ref, the name of the database, then the name of the collection

		collection := client.Database("test").Collection("JSONInfo")

		// Check if the files already exist
		filter := bson.D{{Key: "file_name", Value: result[0].File_name}}
		var searchResult JSONData
		err = collection.FindOne(context.TODO(), filter).Decode(&searchResult)
		if err != mongo.ErrNoDocuments {
			return errorFormated(err, c)
		}
		if searchResult.File_name == result[0].File_name {
			return c.SendString("File Already Exists")
		}

		// send data of one entry
		// I can get access to the UUID through the object it outputs
		insertedObj, err := collection.InsertOne(context.TODO(), result[0])
		if err != nil {
			return errorFormated(err, c)
		}
		insertedObjID := insertedObj.InsertedID.(primitive.ObjectID).Hex()
		fmt.Println(insertedObjID)

		// Initialize a session.
		sess, err := session.NewSessionWithOptions(session.Options{
			Config: aws.Config{
				Region: aws.String("eu-central-1"),
			},
		})
		svc := s3.New(sess)
		if err != nil {
			return errorFormated(err, c)
		}
		bucket := "slim-test-bucket"
		// Check for items in the bucket
		s3Keys := make([]string, 0)
		if err := svc.ListObjectsPagesWithContext(context.TODO(), &s3.ListObjectsInput{
			Bucket: aws.String(bucket),
			Prefix: aws.String(fileNameOnly),
		}, func(o *s3.ListObjectsOutput, b bool) bool {
			for _, o := range o.Contents {
				s3Keys = append(s3Keys, *o.Key)
			}
			return true
		}); err != nil {
			return errorFormated(err, c)
		}

		// If in the bucket we have a folder with the same name, we delete the information on the JSON, and stop the execution
		if len(s3Keys) > 0 {
			filter2 := bson.D{{Key: "file_name", Value: result[0].File_name}}
			_, err = collection.DeleteOne(context.TODO(), filter2)
			if err != nil {
				return errorFormated(err, c)
			}
			return c.SendString("File already exists")
		}

		// Initialize variables to upload to bucket

		path := "./output/" + fileNameOnly + "/img"
		iter := NewDirectoryIterator(bucket, path)
		uploader := s3manager.NewUploader(sess)

		// Upload to Bucket
		if err := uploader.UploadWithIterator(aws.BackgroundContext(), iter); err != nil {
			return errorFormated(err, c)
		}
		fmt.Printf("Successfully uploaded %q to %q", path, bucket)
		// Delete the created files
		err = os.RemoveAll("./output")
		if err != nil {
			return errorFormated(err, c)
		}
		return c.SendString("Finished")

	})

	log.Fatal(app.Listen(":4000"))
}
