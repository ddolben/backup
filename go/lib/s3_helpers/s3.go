package s3_helpers

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func UploadFile(client *s3.Client, bucket string, key string, localPath string) error {
	localFile, err := os.Open(localPath)
	if err != nil {
		return fmt.Errorf("failed to open local file %q", localPath)
	}
	defer localFile.Close()

	_, err = client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: &bucket,
		Key:    &key,
		Body:   localFile,
	})
	if err != nil {
		return fmt.Errorf("failed to upload local file %q to %q", localPath, key)
	}
	return nil
}

func DownloadFile(client *s3.Client, bucket string, key string, localPath string) error {
	objectDataOutput, err := client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &key,
	})
	if err != nil {
		return fmt.Errorf("failed to download file %q: %s", key, err)
	}

	// Create intermediate directories if necessary
	if err != os.MkdirAll(filepath.Dir(localPath), os.ModePerm) {
		return fmt.Errorf("failed to create local file %q: %s", localPath, err)
	}

	localFile, err := os.Create(localPath)
	if err != nil {
		return fmt.Errorf("failed to create local file %q: %s", localPath, err)
	}
	defer localFile.Close()
	if _, err := io.Copy(localFile, objectDataOutput.Body); err != nil {
		return fmt.Errorf("failed to write to local file %q: %s", localPath, err)
	}
	return nil
}
