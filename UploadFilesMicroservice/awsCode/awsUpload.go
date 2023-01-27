package awsupload

import (
	"fmt"
	"os"
	"path/filepath"

	channeltypes "uploadfilesmicroservice/typeDef"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

func UploadDirToS3(bucket string, jobs <-chan channeltypes.ImagePath, results chan<- channeltypes.Result) {
	for imgPath := range jobs {
		loc, err := putInS3(imgPath.FilePath, bucket)
		if err != nil {
			fmt.Println(err.Error())
		}
		results <- channeltypes.Result{ReturnMessage: loc}
	}
}

func putInS3(pathOfFile string, bucket string) (location string, err error) {
	var folder string
	if err != nil {
		fmt.Printf("Error loading .env file")
	}
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String("eu-central-1")},
	)
	if err != nil {
		fmt.Println(err.Error())
	}
	file, err := os.Open(pathOfFile)
	if err != nil {
		fmt.Println(err.Error())
	}
	if filepath.Ext(file.Name()) == ".png" {
		folder = "/img/"
	}
	if filepath.Ext(file.Name()) == ".psf" {
		folder = "/psf/"
	}
	path := folder + filepath.Base(file.Name())
	defer file.Close()
	uploader := s3manager.NewUploader(sess)
	uploadOutput, err := uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(path),
		Body:   file,
	})
	if err != nil {
		return "", err
	}
	return uploadOutput.Location, nil

}
