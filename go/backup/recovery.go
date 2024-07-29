package backup

import (
	"context"
	"log"
	"os"
	"path/filepath"

	"local/backup/s3_helpers"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func RecoverFiles(cfg *aws.Config, bucket string, localRoot string) {
	log.Println("> Recovering files")

	// Create an Amazon S3 service client
	client := s3.NewFromConfig(*cfg)

	// Get the first page of results for ListObjectsV2 for a bucket
	output, err := client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		log.Fatal(err)
	}

	for _, object := range output.Contents {
		log.Printf("key=%s size=%d", aws.ToString(object.Key), object.Size)
		log.Printf("downloading...")
		localPath := filepath.Join(localRoot, *object.Key)
		if err := s3_helpers.DownloadFile(client, bucket, *object.Key, localPath); err != nil {
			log.Fatalf("%s", err)
		}
		if filepath.Base(localPath) == "_files.tar.gz" {
			log.Printf("extracting files from archive %q", localPath)
			unTar(localPath)
			// Delete the archive
			log.Printf("deleting archive %q", localPath)
			os.Remove(localPath)
		}
		log.Printf("downloaded %q to local file %q", *object.Key, localPath)
	}

	log.Println("< Recovering files")
}
