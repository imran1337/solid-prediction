package awsupload

import (
	"fmt"
	"os"
	"path/filepath"

	typeDef "uploadfilesmicroservice/typeDef"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

func SearchAndMatch(svc *s3.S3, bucket string, name string) (matchFound bool) {
	resp, err := svc.ListObjectsV2(&s3.ListObjectsV2Input{Bucket: aws.String(bucket), Prefix: aws.String("preset/")})
	if err != nil {
		fmt.Println(err.Error())
	}
	for _, item := range resp.Contents {
		if filepath.Base(name) == filepath.Base(*item.Key) {
			return true
		}
	}
	return false
}

func UploadDirToS3(bucket string, jobs <-chan typeDef.ImagePath, results chan<- typeDef.Result, sess *session.Session, svc *s3.S3) {
	for imgPath := range jobs {
		loc, err := putInS3(imgPath.FilePath, bucket, sess, svc)
		if err != nil {
			fmt.Println(err.Error())
		}
		results <- typeDef.Result{ReturnMessage: loc}
	}
}

func putInS3(pathOfFile string, bucket string, sess *session.Session, svc *s3.S3) (location string, err error) {
	var folder string
	if err != nil {
		fmt.Printf("Error loading .env file")
	}

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
	if filepath.Ext(file.Name()) == ".preset" {
		folder = "/preset/"
		if SearchAndMatch(svc, bucket, filepath.Base(file.Name())) {
			fmt.Println("This is a duplicate: " + filepath.Base(file.Name()))
			return filepath.Base(file.Name()), nil
		}
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
