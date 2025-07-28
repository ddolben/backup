package backup

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/fs"
	"local/backup/lib/logging"
	"local/backup/lib/util"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

const minioUrl = "http://localhost:9000"

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randSeq(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func createTestFile(filename string, size int) error {
	dir := filepath.Dir(filename)
	if len(dir) > 0 {
		if err := os.MkdirAll(dir, os.ModePerm); err != nil {
			return err
		}
	}
	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	contents := strings.NewReader(randSeq(size))
	_, err = io.Copy(f, contents)
	return err
}

const chunkSize = 64000

func compareFiles(file1, file2 string) error {
	f1, err := os.Open(file1)
	if err != nil {
		log.Fatal(err)
	}
	defer f1.Close()

	f2, err := os.Open(file2)
	if err != nil {
		log.Fatal(err)
	}
	defer f2.Close()

	// Check that the files' modtimes match
	info1, err := f1.Stat()
	if err != nil {
		log.Fatal(err)
	}
	info2, err := f2.Stat()
	if err != nil {
		log.Fatal(err)
	}
	// As far as I can tell, modtimes are only guaranteed to be equal to the second.
	if !info1.ModTime().Truncate(time.Second).Equal(info2.ModTime().Truncate(time.Second)) {
		return fmt.Errorf("files have different modtimes: %v != %v", info1.ModTime(), info2.ModTime())
	}

	for {
		b1 := make([]byte, chunkSize)
		_, err1 := f1.Read(b1)

		b2 := make([]byte, chunkSize)
		_, err2 := f2.Read(b2)

		if err1 != nil || err2 != nil {
			if err1 == io.EOF && err2 == io.EOF {
				return nil
			} else if err1 == io.EOF || err2 == io.EOF {
				return fmt.Errorf("file contents are not equal")
			} else {
				log.Fatal(err1, err2)
			}
		}

		if !bytes.Equal(b1, b2) {
			return fmt.Errorf("file contents are not equal")
		}
	}
}

func compareDirectories(baseDir string, recoveryDir string, t *testing.T) {
	// Track all seen files so we also get a deletion check.
	unexpectedFiles := make(map[string]struct{})
	err := filepath.WalkDir(recoveryDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if d.IsDir() {
			// Skip directories
			return nil
		}

		relativePath, err := filepath.Rel(recoveryDir, path)
		if err != nil {
			return err
		}

		unexpectedFiles[relativePath] = struct{}{}
		return nil
	})
	if err != nil {
		t.Fatalf("error tracking unexpected files %+v", err)
	}

	err = filepath.WalkDir(baseDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if d.IsDir() {
			// Skip directories
			return nil
		}

		if filepath.Base(path) == "backup.db" {
			// Skip the backup db
			// TODO: this is janky, what if one of the files to be backed up has this name?
			return nil
		}

		relativePath, err := filepath.Rel(baseDir, path)
		if err != nil {
			return err
		}
		delete(unexpectedFiles, relativePath)
		if err := compareFiles(path, filepath.Join(recoveryDir, relativePath)); err != nil {
			t.Errorf("files are not equal at path %q: %v", relativePath, err)
			return fmt.Errorf("files are not equal at path %q: %v", relativePath, err)
		}
		return nil
	})

	if err != nil {
		t.Fatalf("%+v", err)
	}

	if len(unexpectedFiles) > 0 {
		t.Fatalf("found unexpected files in recovery directory: %+v", unexpectedFiles)
	}
}

func clearBucket(client *s3.Client, bucket string, prefix string) error {
	// Get the first page of results for ListObjectsV2 for a bucket
	output, err := client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	})
	if err != nil {
		return err
	}

	for _, object := range output.Contents {
		log.Printf("deleting %s:%s", bucket, *object.Key)
		_, err := client.DeleteObjects(context.TODO(), &s3.DeleteObjectsInput{
			Bucket: aws.String(bucket),
			Delete: &types.Delete{
				Objects: []types.ObjectIdentifier{
					{
						Key: object.Key,
					},
				},
			},
		})
		if err != nil {
			return err
		}
	}

	return nil
}

const (
	bucket     = "test-bucket"
	prefixBase = "automated-test"
)

type roundTripTestConfig struct {
	Bucket        string
	BackupName    string
	TestBaseDir   string
	DBFile        string
	SizeThreshold int64
	// If true, doesn't clear out the S3 bucket's contents. Useful for running multiple tests in a row
	// to validate dirty checks.
	LeaveBucketContents bool
	S3Prefix            string
	FullS3Prefix        string
	Cleanup             func()
}

func getDefaultTestConfig() *roundTripTestConfig {
	myPrefix := prefixBase + "-" + randSeq(16)

	testBaseDir, err := os.MkdirTemp("/tmp", "dave-backup-test-")
	must(err)
	testDBDir, err := os.MkdirTemp("/tmp", "dave-backup-db-")
	must(err)

	backupName := "test-backup"
	dbFile := filepath.Join(testDBDir, fmt.Sprintf("%s.db", backupName))

	return &roundTripTestConfig{
		Bucket:        bucket,
		BackupName:    backupName,
		TestBaseDir:   testBaseDir,
		DBFile:        dbFile,
		SizeThreshold: 10,
		S3Prefix:      myPrefix,
		FullS3Prefix:  filepath.Join(myPrefix, backupName),
		Cleanup: func() {
			cfg := GetMinioConfig(minioUrl)
			client := s3.NewFromConfig(*cfg)
			must(clearBucket(client, bucket, myPrefix))

			must(os.RemoveAll(testBaseDir))
			must(os.RemoveAll(testDBDir))
		},
	}
}

func roundTripTest(testConfig *roundTripTestConfig, t *testing.T) {
	testRecoveryDir, err := os.MkdirTemp("/tmp", "dave-recovery-test-")
	must(err)
	testBaseDir := testConfig.TestBaseDir

	defer (func() {
		must(os.RemoveAll(testRecoveryDir))
	})()

	cfg := GetMinioConfig(minioUrl)
	client := s3.NewFromConfig(*cfg)

	if !testConfig.LeaveBucketContents {
		must(clearBucket(client, bucket, testConfig.S3Prefix))
	}

	logger := &logging.DefaultLogger{
		Level: logging.Debug,
	}

	must(BackupFiles(
		logger,
		cfg,
		testConfig.DBFile,
		testBaseDir,
		bucket,
		testConfig.S3Prefix,
		testConfig.BackupName,
		testConfig.SizeThreshold,
		BackupOptions{},
	))
	must(RecoverFiles(
		logger,
		cfg,
		testConfig.DBFile,
		bucket,
		testConfig.S3Prefix,
		testConfig.BackupName,
		testRecoveryDir,
		RecoveryOptions{},
	))

	compareDirectories(testBaseDir, testRecoveryDir, t)

	// Read the backup db and find all of the S3 keys that should exist.
	db, err := NewDB(testConfig.DBFile)
	must(err)
	defer db.Close()

	batchesInDb, err := db.GetExistingBatches(true)
	must(err)

	log.Printf("batches in db:")
	for _, batch := range batchesInDb {
		log.Printf("  batch: %s", batch.Path)
		for _, filename := range batch.Filenames {
			log.Printf("    filename: %s", filename)
		}
	}

	// Make sure no filenames are duplicated across batches.
	seenFilenames := make(map[string]struct{})
	for _, batch := range batchesInDb {
		for _, filename := range batch.Filenames {
			if _, ok := seenFilenames[filename]; ok {
				t.Fatalf("filename %s is duplicated across batches", filename)
			}
			seenFilenames[filename] = struct{}{}
		}
	}

	batchesInS3, err := client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(testConfig.S3Prefix),
	})
	if err != nil {
		t.Fatalf("error listing objects in S3: %+v", err)
	}

	log.Printf("batches in S3:")
	for _, object := range batchesInS3.Contents {
		log.Printf("  batch: %s", *object.Key)
	}

	// Make a map of all the objects in S3 so we can check for unexpected ones.
	unexpectedBatches := make(map[string]struct{})
	for _, object := range batchesInS3.Contents {
		unexpectedBatches[*object.Key] = struct{}{}
	}

	// Check that the DB is present in S3, and delete it from the map.
	dbKey := fmt.Sprintf("%s/%s.db.gz", testConfig.S3Prefix, testConfig.BackupName)
	if _, ok := unexpectedBatches[dbKey]; !ok {
		t.Fatalf("backup db not found in S3: %s", dbKey)
	}
	delete(unexpectedBatches, dbKey)

	// Make sure the objects in S3 match the batches in the db.
	for _, batch := range batchesInDb {
		var batchKey string
		if batch.IsSingleFile {
			batchKey = fmt.Sprintf("%s/%s.tar.gz", testConfig.FullS3Prefix, batch.Path)
		} else {
			if batch.Path == "." {
				batchKey = fmt.Sprintf("%s/_files.tar.gz", testConfig.FullS3Prefix)
			} else {
				batchKey = fmt.Sprintf("%s/%s/_files.tar.gz", testConfig.FullS3Prefix, batch.Path)
			}
		}
		log.Printf("observed batchKey: %s", batchKey)
		delete(unexpectedBatches, batchKey)
		found := false
		for _, object := range batchesInS3.Contents {
			if *object.Key == batchKey {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("batch %s not found in S3", batch.Path)
		}
	}
	if len(unexpectedBatches) > 0 {
		t.Fatalf("found unexpected batches in S3: %+v", unexpectedBatches)
	}
}

func assertBatchCount(t *testing.T, dbFile string, s3Prefix string, expected int) {
	// Check the batch count in the DB
	db, err := NewDB(dbFile)
	must(err)
	batchesInDb, err := db.GetExistingBatches(true)
	must(err)
	if len(batchesInDb) != expected {
		t.Fatalf("expected %d batch(es) in the db, got: %+v", expected, batchesInDb)
	}

	// Check the batch count in S3
	cfg := GetMinioConfig(minioUrl)
	client := s3.NewFromConfig(*cfg)
	output, err := client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(s3Prefix + "/"),
	})
	if err != nil {
		log.Fatal(err)
	}
	if len(output.Contents) != expected {
		t.Fatalf("expected %d S3 file(s), got: %+v", expected, util.Map(output.Contents, func(o types.Object) string {
			return *o.Key
		}))
	}
}
