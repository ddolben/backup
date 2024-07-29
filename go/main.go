package main

import (
	"flag"

	"local/backup/backup"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
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
	fMetaDb := flag.String("db", "backup.db", "database location for local cache storage")
	fRootDir := flag.String("dir", ".", "root directory for backup operation")
	fMaxDepth := flag.Int("max_depth", -1, "max subfolder depth to recurse into before just archiving the whole tree (-1 means no max)")
	fBucket := flag.String("bucket", "test-bucket", "S3 bucket")
	fIndividualSizeThreshold := flag.Int("size_threshold", 0, "size threshold, in bytes, above which files get backed up individually")
	fDoRecover := flag.Bool("recover", false, "If true, recovers FROM the remote location TO the local location")
	flag.Parse()

	cfg := minioConfig()

	if *fDoRecover {
		backup.RecoverFiles(cfg, *fBucket, *fRootDir)
	} else {
		backup.BackupFiles(cfg, *fMetaDb, *fRootDir, *fBucket, *fMaxDepth, *fIndividualSizeThreshold)
	}
}
