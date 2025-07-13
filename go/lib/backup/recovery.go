package backup

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"local/backup/lib/s3_helpers"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// TODO: return errors vs. Fatal-ing
func RecoverFiles(cfg *aws.Config, bucket string, prefix string, localRoot string) error {
	log.Println("> Recovering files")

	// Create an Amazon S3 service client
	client := s3.NewFromConfig(*cfg)

	if len(prefix) == 0 {
		return fmt.Errorf("S3 key prefix is required")
	}

	keyPrefix := prefix
	if !strings.HasSuffix(prefix, "/") {
		keyPrefix += "/"
	}

	// Get the first page of results for ListObjectsV2 for a bucket
	output, err := client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		// Return only files with the given prefix
		Prefix: aws.String(prefix),
	})
	if err != nil {
		log.Fatal(err)
	}

	for _, object := range output.Contents {
		log.Printf("key=%s size=%d", aws.ToString(object.Key), object.Size)
		log.Printf("downloading...")
		localPath := filepath.Join(localRoot, strings.TrimPrefix(*object.Key, keyPrefix))
		if err := s3_helpers.DownloadFile(client, bucket, *object.Key, localPath); err != nil {
			log.Fatalf("%s", err)
		}
		log.Printf("downloaded %q to local file %q", *object.Key, localPath)
		if filepath.Base(localPath) == "_files.tar.gz" {
			log.Printf("extracting files from archive %q", localPath)
			err := unTar(localPath, filepath.Dir(localPath))
			if err != nil {
				log.Fatalf("failed to extract files from archive %q: %v", localPath, err)
			}
			// Delete the archive
			log.Printf("deleting archive %q", localPath)
			os.Remove(localPath)
		} else {
			log.Printf("extracting single file %q", localPath)
			err := decompressFile(localPath, filepath.Dir(localPath))
			if err != nil {
				log.Fatalf("failed to decompress file %q: %v", localPath, err)
			}
			log.Printf("deleted compressed file %q", localPath)
			os.Remove(localPath)
		}
	}

	log.Println("< Recovering files")

	return nil
}
