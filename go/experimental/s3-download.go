package main

import (
	"context"
	"log"
	"path/filepath"

	"local/backup/s3_helpers"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func minioConfig() *aws.Config {
	const defaultRegion = "us-east-1"
	staticResolver := aws.EndpointResolverFunc(func(service, region string) (aws.Endpoint, error) {
		return aws.Endpoint{
			PartitionID:       "aws",
			URL:               "http://localhost:9000",
			SigningRegion:     defaultRegion,
			HostnameImmutable: true,
		}, nil
	})

	cfg := &aws.Config{
		Region:           defaultRegion,
		Credentials:      credentials.NewStaticCredentialsProvider("minio", "minio123", ""),
		EndpointResolver: staticResolver,
	}
	return cfg
}

func main() {
	// Load the Shared AWS Configuration (~/.aws/config)
	//cfg, err := config.LoadDefaultConfig(context.TODO())
	//if err != nil {
	//	log.Fatal(err)
	//}
	cfg := minioConfig()

	// Create an Amazon S3 service client
	client := s3.NewFromConfig(*cfg)

	bucket := "test-bucket"

	// Get the first page of results for ListObjectsV2 for a bucket
	output, err := client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		log.Fatal(err)
	}

	localRoot := "."
	for _, object := range output.Contents {
		log.Printf("key=%s size=%d", aws.ToString(object.Key), object.Size)
		log.Printf("downloading...")
		localPath := filepath.Join(localRoot, *object.Key)
		if err := s3_helpers.DownloadFile(client, bucket, *object.Key, localPath); err != nil {
			log.Fatalf("%s", err)
		}
		log.Printf("downloaded %q to local file %q", *object.Key, localPath)
	}
}
