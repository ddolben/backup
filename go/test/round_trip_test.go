package test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/fs"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"local/backup/backup"
	"local/backup/logging"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// TODO: test file deletion when it _doesn't_ change the batching strategy
// TODO: test that changing the size threshold doesn't mess anything up

const minioUrl = "http://localhost:9000"

func init() {
	rand.Seed(time.Now().UnixNano())
}

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

func compareFiles(file1, file2 string) bool {
	// Check file size ...

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

	for {
		b1 := make([]byte, chunkSize)
		_, err1 := f1.Read(b1)

		b2 := make([]byte, chunkSize)
		_, err2 := f2.Read(b2)

		if err1 != nil || err2 != nil {
			if err1 == io.EOF && err2 == io.EOF {
				return true
			} else if err1 == io.EOF || err2 == io.EOF {
				return false
			} else {
				log.Fatal(err1, err2)
			}
		}

		if !bytes.Equal(b1, b2) {
			return false
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
		if !compareFiles(path, filepath.Join(recoveryDir, relativePath)) {
			t.Errorf("files are not equal: %q", relativePath)
			return fmt.Errorf("files are not equal: %q", relativePath)
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

func clearBucket(client *s3.Client, bucket string) error {
	// Get the first page of results for ListObjectsV2 for a bucket
	output, err := client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
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

type roundTripTestConfig struct {
	SizeThreshold int64
	// If true, doesn't clear out the S3 bucket's contents. Useful for running multiple tests in a row
	// to validate dirty checks.
	LeaveBucketContents bool
}

func getDefaultTestConfig() *roundTripTestConfig {
	return &roundTripTestConfig{
		SizeThreshold: 10,
	}
}

const (
	bucket = "test-bucket"
	prefix = "automated-test"
)

func roundTripTest(testBaseDir string, testConfig *roundTripTestConfig, t *testing.T) {
	testRecoveryDir, err := os.MkdirTemp("/tmp", "dave-recovery-test-")
	must(err)

	defer (func() {
		must(os.RemoveAll(testRecoveryDir))
	})()

	cfg := backup.GetMinioConfig(minioUrl)
	dbFile := filepath.Join(testBaseDir, "backup.db")

	if !testConfig.LeaveBucketContents {
		client := s3.NewFromConfig(*cfg)
		must(clearBucket(client, bucket))
	}

	logger := &logging.DefaultLogger{
		Level: logging.Debug,
	}

	// a and b but not c will be tarred/gzipped
	must(backup.BackupFiles(logger, cfg, dbFile, testBaseDir, bucket, prefix, testConfig.SizeThreshold, false))
	must(backup.RecoverFiles(cfg, bucket, prefix, testRecoveryDir))

	compareDirectories(testBaseDir, testRecoveryDir, t)
}

func TestBasicRoundTrip(t *testing.T) {
	// Create test directory and write a bunch of files
	testBaseDir, err := os.MkdirTemp("/tmp", "dave-backup-test-")
	must(err)

	defer (func() {
		must(os.RemoveAll(testBaseDir))
	})()

	must(createTestFile(filepath.Join(testBaseDir, "a.txt"), 5))
	must(createTestFile(filepath.Join(testBaseDir, "b.txt"), 9))
	must(createTestFile(filepath.Join(testBaseDir, "c.txt"), 25))

	roundTripTest(testBaseDir, getDefaultTestConfig(), t)
}

func TestRoundTripWithSubdirectories(t *testing.T) {
	// Create test directory and write a bunch of files
	testBaseDir, err := os.MkdirTemp("/tmp", "dave-backup-test-")
	must(err)

	defer (func() {
		must(os.RemoveAll(testBaseDir))
	})()

	must(createTestFile(filepath.Join(testBaseDir, "a.txt"), 5))
	must(createTestFile(filepath.Join(testBaseDir, "b.txt"), 9))
	must(createTestFile(filepath.Join(testBaseDir, "c.txt"), 25))

	must(createTestFile(filepath.Join(testBaseDir, "subdir-1/a.txt"), 5))
	must(createTestFile(filepath.Join(testBaseDir, "subdir-1/b.txt"), 9))
	must(createTestFile(filepath.Join(testBaseDir, "subdir-1/c.txt"), 25))

	must(createTestFile(filepath.Join(testBaseDir, "subdir-2/a.txt"), 5))
	must(createTestFile(filepath.Join(testBaseDir, "subdir-2/b.txt"), 9))
	must(createTestFile(filepath.Join(testBaseDir, "subdir-2/c.txt"), 25))

	roundTripTest(testBaseDir, getDefaultTestConfig(), t)
}

func TestRoundTripWithDeepSubdirectories(t *testing.T) {
	// Create test directory and write a bunch of files
	testBaseDir, err := os.MkdirTemp("/tmp", "dave-backup-test-")
	must(err)

	defer (func() {
		must(os.RemoveAll(testBaseDir))
	})()

	must(createTestFile(filepath.Join(testBaseDir, "a.txt"), 5))
	must(createTestFile(filepath.Join(testBaseDir, "b.txt"), 9))
	must(createTestFile(filepath.Join(testBaseDir, "c.txt"), 25))

	must(createTestFile(filepath.Join(testBaseDir, "subdir-1/one/two/three/a.txt"), 5))
	must(createTestFile(filepath.Join(testBaseDir, "subdir-1/four/five/six/b.txt"), 9))
	must(createTestFile(filepath.Join(testBaseDir, "subdir-1/seven/eight/nine/c.txt"), 25))

	must(createTestFile(filepath.Join(testBaseDir, "subdir-2/with/many/directories/a.txt"), 5))
	must(createTestFile(filepath.Join(testBaseDir, "subdir-2/with/many/directories/b.txt"), 9))
	must(createTestFile(filepath.Join(testBaseDir, "subdir-2/with/many/directories/c.txt"), 25))

	config := getDefaultTestConfig()
	roundTripTest(testBaseDir, config, t)
}

func TestRoundTripWithDeepSubdirectoriesBeyondThreshold(t *testing.T) {
	// Create test directory and write a bunch of files
	testBaseDir, err := os.MkdirTemp("/tmp", "dave-backup-test-")
	must(err)

	defer (func() {
		must(os.RemoveAll(testBaseDir))
	})()

	must(createTestFile(filepath.Join(testBaseDir, "a.txt"), 5))
	must(createTestFile(filepath.Join(testBaseDir, "b.txt"), 9))
	must(createTestFile(filepath.Join(testBaseDir, "c.txt"), 25))

	must(createTestFile(filepath.Join(testBaseDir, "subdir-1/one/two/three/a.txt"), 5))
	must(createTestFile(filepath.Join(testBaseDir, "subdir-1/four/five/six/b.txt"), 9))
	must(createTestFile(filepath.Join(testBaseDir, "subdir-1/seven/eight/nine/c.txt"), 25))

	must(createTestFile(filepath.Join(testBaseDir, "subdir-2/with/many/directories/a.txt"), 5))
	must(createTestFile(filepath.Join(testBaseDir, "subdir-2/with/many/directories/b.txt"), 9))
	must(createTestFile(filepath.Join(testBaseDir, "subdir-2/with/many/directories/c.txt"), 25))

	roundTripTest(testBaseDir, getDefaultTestConfig(), t)
}

func TestRoundTrip_WithLargeSizeThreshold(t *testing.T) {
	// Create test directory and write a bunch of files
	testBaseDir, err := os.MkdirTemp("/tmp", "dave-backup-test-")
	must(err)

	defer (func() {
		must(os.RemoveAll(testBaseDir))
	})()

	must(createTestFile(filepath.Join(testBaseDir, "a.txt"), 5))
	must(createTestFile(filepath.Join(testBaseDir, "b.txt"), 9))
	must(createTestFile(filepath.Join(testBaseDir, "c.txt"), 25))

	must(createTestFile(filepath.Join(testBaseDir, "subdir-1/one/two/three/a.txt"), 5))
	must(createTestFile(filepath.Join(testBaseDir, "subdir-1/four/five/six/b.txt"), 9))
	must(createTestFile(filepath.Join(testBaseDir, "subdir-1/four/five/six/big.txt"), 2000))
	must(createTestFile(filepath.Join(testBaseDir, "subdir-1/seven/eight/nine/c.txt"), 25))

	config := getDefaultTestConfig()
	config.SizeThreshold = 1000
	roundTripTest(testBaseDir, config, t)

	// Remove the large file down the three, causing the entire directory hierarchy to collapse into
	// one batch. Also tests that file deletion is working properly.
	must(os.Remove(filepath.Join(testBaseDir, "subdir-1/four/five/six/big.txt")))

	// Run the test again, _without_ clearing the bucket (so we effectively get the same behavior as a
	// non-fresh run in real life).
	config.LeaveBucketContents = true
	roundTripTest(testBaseDir, config, t)

	// Check manually for extraneous S3 files. There should now only be one in this prefix, since the
	// smaller batches from the first run should have been cleaned up.
	cfg := backup.GetMinioConfig(minioUrl)
	client := s3.NewFromConfig(*cfg)
	output, err := client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	})
	if err != nil {
		log.Fatal(err)
	}
	if len(output.Contents) > 1 {
		t.Fatalf("expected just 1 S3 file, got: %+v", output.Contents)
	}
}
