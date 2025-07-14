package backup

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"local/backup/lib/logging"
	"local/backup/lib/s3_helpers"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type RecoveryOptions struct {
	Force bool
}

// TODO: return errors vs. Fatal-ing
func RecoverFiles(
	logger logging.Logger,
	cfg *aws.Config,
	dbFile string,
	bucket string,
	prefixBase string,
	name string,
	localRoot string,
	options RecoveryOptions,
) error {
	prefix := filepath.Join(prefixBase, name)

	// Create an Amazon S3 service client
	client := s3.NewFromConfig(*cfg)

	if len(prefix) == 0 {
		return fmt.Errorf("S3 key prefix is required")
	}

	// Download the backup db from S3 and check if any files have changed since the last time we did a
	// backup or recovery.
	changes, err := downloadAndCompareDB(logger, client, dbFile, bucket, prefixBase, name)
	if err != nil {
		log.Fatalf("error downloading and comparing db: %v", err)
	}
	if len(changes) > 0 {
		logger.Infof("files have changed in storage since the last backup or recovery, aborting:")
		printChanges(changes)
		if options.Force {
			logger.Infof("forcing recovery despite changes in storage")
		} else {
			return fmt.Errorf("files have changed in storage since the last backup or recovery")
		}
	}

	keyPrefix := prefix
	if !strings.HasSuffix(keyPrefix, "/") {
		keyPrefix += "/"
	}

	logger.Verbosef("> Recovering files from %s", keyPrefix)

	// Download the backup db from S3 so we can compare it to the remote DB next time we do a recovery.
	remoteDBFile, err := downloadDB(logger, client, bucket, prefixBase, name, filepath.Dir(dbFile))
	if err != nil {
		return fmt.Errorf("failed to download remote db file: %v", err)
	}
	if remoteDBFile != dbFile {
		logger.Verbosef("renaming remote db file %q to %q", remoteDBFile, dbFile)
		os.Rename(remoteDBFile, dbFile)
	}
	logger.Verbosef("downloaded remote db file to %q", dbFile)

	// Get the first page of results for ListObjectsV2 for a bucket
	output, err := client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		// Return only files with the given prefix
		Prefix: aws.String(keyPrefix),
	})
	if err != nil {
		log.Fatal(err)
	}

	// TODO: integrity check between files and db?
	// TODO: only download changes?

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
			//_, err := decompressFile(localPath, filepath.Dir(localPath))
			err := unTar(localPath, filepath.Dir(localPath))
			if err != nil {
				log.Fatalf("failed to decompress file %q: %v", localPath, err)
			}
			log.Printf("deleting compressed file %q", localPath)
			os.Remove(localPath)
		}
	}

	// Go through the db and update all the files' modtimes to match the remote DB.
	// TODO: can we just get the decompression utility to do this?

	log.Println("< Recovering files")

	return nil
}
