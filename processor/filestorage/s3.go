package filestorage

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"os"
)

type AWSS3 struct {
	region   string
	bucket   string
	uploader *s3manager.Uploader
	S3Client *s3.S3
}

func NewAWSS3(region string, bucket string) *AWSS3 {
	s3Session, err := session.NewSession(&aws.Config{
		Region: aws.String(region)})
	if err != nil {
		return nil
	}

	return &AWSS3{bucket: bucket,
		uploader: s3manager.NewUploader(s3Session),
		S3Client: s3.New(s3Session),
	}
}

// StoreFile uploads srcpath to the AWS S3 bucket and then deletes srcpath
func (b AWSS3) StoreFile(srcpath string, destpath string) error {
	f, err := os.Open(srcpath)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = b.uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(destpath),
		Body:   f,
	})
	if err != nil {
		return err
	}
	os.Remove(srcpath)
	return nil
}

// DeleteFile deletes filepath from the AWS S3 bucket
func (b AWSS3) DeleteFile(filepath string) error {
	_, err := b.S3Client.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(filepath),
	})
	if err != nil {
		return err
	}
	return nil
}

// FileExists returns true if the file exists, false otherwise
func (b AWSS3) FileExists(filepath string) bool {
	_, err := b.S3Client.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(filepath),
	})
	return err == nil
}
