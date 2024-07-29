package main

import (
	"context"
	"crypto/md5"
	"database/sql"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

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

func getFileHash(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()

	h := md5.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}

	return fmt.Sprintf("%x", h.Sum(nil)), nil
}

func doesFileNeedBackup(db *DB, path string, info fs.FileInfo) (bool, error) {
	fi, err := db.GetFileInfo(path)
	if err != nil && err != sql.ErrNoRows {
		return false, err
	}

	if err == sql.ErrNoRows {
		return true, nil
	}
	if !info.ModTime().Truncate(time.Millisecond).After(fi.ModTime.Truncate(time.Millisecond)) {
		return false, nil
	}

	hash, err := getFileHash(path)
	if err != nil {
		return false, err
	}
	if hash == fi.Hash {
		return false, nil
	}

	return true, nil
}

type QueueItem struct {
	Path  string
	Depth int
}

type BackupRequest struct {
	Path             string
	IsDir            bool
	Recurse          bool
	DirtySubFiles    []string
	SubFilesToBackup []string
}

func doBackupFile(path string) bool {
	dir := filepath.Dir(path)
	filename := filepath.Base(path)

	// TODO: this check is kinda janky
	if !strings.Contains(dir, "/") && filename == "backup.db" {
		return false
	}
	return true
}

func getFilesToBackup(db *DB, root string, maxDepth int, sizeThreshold int) ([]*BackupRequest, error) {
	dirQueue := []QueueItem{{Path: root, Depth: 0}}
	backupRequests := []*BackupRequest{}

	// Scan files and compare mod times
	for len(dirQueue) > 0 {
		dir := dirQueue[0]
		dirQueue = dirQueue[1:]

		req := &BackupRequest{
			Path:  dir.Path,
			IsDir: true,
		}

		log.Printf("scanning dir %q depth %d", dir.Path, dir.Depth)

		if maxDepth > 0 && dir.Depth > maxDepth {
			// Check if directory needs backup.
			files, err := getFilesToBackup(db, dir.Path, -1, sizeThreshold)
			if err != nil {
				return nil, err
			}
			if len(files) > 0 {
				// This is a terminal directory (i.e. we just back up the whole thing as an archive)
				req.Recurse = true
				for _, f := range files {
					req.DirtySubFiles = append(req.DirtySubFiles, f.Path)
				}
				backupRequests = append(backupRequests, req)
			}
			continue
		}

		files, err := os.ReadDir(dir.Path)
		if err != nil {
			log.Fatalf("error scanning directory: %v", err)
		}

		for _, file := range files {
			path := filepath.Join(dir.Path, file.Name())

			if file.IsDir() {
				dirQueue = append(dirQueue, QueueItem{
					Path:  path,
					Depth: dir.Depth + 1,
				})
				continue
			}

			info, err := file.Info()
			if err != nil {
				log.Fatal(err)
			}

			if doBackupFile(path) {
				dirty, err := doesFileNeedBackup(db, path, info)
				if err != nil {
					log.Fatal(err)
				}

				log.Printf("%s (%d bytes) - %v", path, info.Size(), info.ModTime())
				if info.Size() > int64(sizeThreshold) {
					if dirty {
						// If the file meets the given criteria and is dirty, back it up individually
						backupRequests = append(backupRequests, &BackupRequest{
							Path:  path,
							IsDir: false,
						})
					} else {
						log.Printf("%s already backed up, skipping", path)
					}
				} else {
					// Make sure to include _all_ files below the individual backup threshold - if this directory
					// contains _any_ dirty files below the size threshold, we want to make sure we include _all_
					// of them in the archive.
					req.SubFilesToBackup = append(req.SubFilesToBackup, path)
					if dirty {
						req.DirtySubFiles = append(req.DirtySubFiles, path)
					}
				}
			} else {
				log.Printf("skipping file %s", path)
			}
		}

		if len(req.DirtySubFiles) > 0 {
			backupRequests = append(backupRequests, req)
		} else {
			log.Printf("directory %q has no dirty files, skipping", req.Path)
		}
	}
	return backupRequests, nil
}

func markFile(db *DB, path string) error {
	info, err := os.Stat(path)
	if err != nil {
		log.Fatalf("error stat-ing file: %v", err)
	}
	hash, err := getFileHash(path)
	if err != nil {
		log.Fatalf("error hashing file: %v", err)
	}

	return db.MarkFile(path, info.ModTime(), hash)
}

func recoverFiles(bucket string, localRoot string) {
	log.Println("> Recovering files")

	// Create an Amazon S3 service client
	cfg := minioConfig()
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

func backupFiles(dbFile string, localRoot string, bucket string, maxDepth int, fileSizeThreshold int) {
	// Load the db
	db, err := newDB(dbFile)
	if err != nil {
		log.Fatalf("error loading db: %v", err)
	}

	// Create an Amazon S3 service client
	cfg := minioConfig()
	client := s3.NewFromConfig(*cfg)

	log.Println("> Scanning files")
	paths, err := getFilesToBackup(db, localRoot, maxDepth, fileSizeThreshold)
	if err != nil {
		log.Fatalf("error finding files to backup: %v", err)
	}
	log.Println("< Scanning files")

	log.Println("> FOUND FILES")
	for _, path := range paths {
		if path.IsDir {
			log.Printf("d %s", path.Path)
			for _, file := range path.DirtySubFiles {
				log.Printf("  %s", file)
			}
		} else {
			log.Printf("f %s", path.Path)
		}
	}
	log.Println("< FOUND FILES")

	log.Println("> Backing up files")
	for _, path := range paths {

		// TODO: strip path so it's relative to the root
		if path.IsDir {
			log.Printf("Backing up directory: %s, dirty files:", path.Path)
			backupDirectory(client, bucket, path.Path, path.SubFilesToBackup)
			for _, f := range path.DirtySubFiles {
				markFile(db, f)
			}
			continue
		}

		log.Printf("Backing up file: %s", path.Path)
		// TODO: strip path so it's relative to the root
		backupFile(client, bucket, path.Path)
		err := markFile(db, path.Path)
		if err != nil {
			log.Fatalf("error marking file as processed: %v", err)
		}
	}
	log.Println("< Backing up files")
}

func main() {
	fMetaDb := flag.String("db", "backup.db", "database location for local cache storage")
	fRootDir := flag.String("dir", ".", "root directory for backup operation")
	fMaxDepth := flag.Int("max_depth", -1, "max subfolder depth to recurse into before just archiving the whole tree (-1 means no max)")
	fBucket := flag.String("bucket", "test-bucket", "S3 bucket")
	fIndividualSizeThreshold := flag.Int("size_threshold", 0, "size threshold, in bytes, above which files get backed up individually")
	fDoRecover := flag.Bool("recover", false, "If true, recovers FROM the remote location TO the local location")
	flag.Parse()

	if *fDoRecover {
		recoverFiles(*fBucket, *fRootDir)
	} else {
		backupFiles(*fMetaDb, *fRootDir, *fBucket, *fMaxDepth, *fIndividualSizeThreshold)
	}
}
