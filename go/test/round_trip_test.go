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

	"local/backup/backup"
	"local/backup/logging"
	"local/backup/util"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// TODO: test file deletion when it _doesn't_ change the batching strategy
// TODO: test that changing the size threshold doesn't mess anything up

const minioUrl = "http://localhost:9000"

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func TestRoundTrip_Basic(t *testing.T) {
	// Create test directory and write a bunch of files
	testBaseDir, err := os.MkdirTemp("/tmp", "dave-backup-test-")
	must(err)

	defer (func() {
		must(os.RemoveAll(testBaseDir))
	})()

	must(createTestFile(filepath.Join(testBaseDir, "a.txt"), 5))
	must(createTestFile(filepath.Join(testBaseDir, "b.txt"), 9))
	must(createTestFile(filepath.Join(testBaseDir, "c.txt"), 25))

	config := getDefaultTestConfig()
	defer config.Cleanup()
	roundTripTest(testBaseDir, config, t)
}

func TestRoundTrip_WithSubdirectories_AllSingleFileBatches(t *testing.T) {
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

	config := getDefaultTestConfig()
	defer config.Cleanup()
	roundTripTest(testBaseDir, config, t)
}

func TestRoundTrip_WithDeepSubdirectories_AllSingleFileBatches(t *testing.T) {
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
	defer config.Cleanup()
	roundTripTest(testBaseDir, config, t)
}

func TestRoundTrip_SomeMultiFileBatches(t *testing.T) {
	// Create test directory and write a bunch of files
	testBaseDir, err := os.MkdirTemp("/tmp", "dave-backup-test-")
	must(err)

	defer (func() {
		must(os.RemoveAll(testBaseDir))
	})()

	// These should get grouped
	must(createTestFile(filepath.Join(testBaseDir, "a.txt"), 5))
	must(createTestFile(filepath.Join(testBaseDir, "b.txt"), 9))
	must(createTestFile(filepath.Join(testBaseDir, "c.txt"), 25))

	// These should _not_ get grouped because one of the files is too big
	must(createTestFile(filepath.Join(testBaseDir, "subdir-1/one/two/three/a.txt"), 5))
	must(createTestFile(filepath.Join(testBaseDir, "subdir-1/four/five/six/b.txt"), 9))
	must(createTestFile(filepath.Join(testBaseDir, "subdir-1/four/five/six/big.txt"), 2000))
	must(createTestFile(filepath.Join(testBaseDir, "subdir-1/seven/eight/nine/c.txt"), 25))

	// These should get grouped together separately from the top-level files
	must(createTestFile(filepath.Join(testBaseDir, "subdir-2/one/two/three/a.txt"), 5))
	must(createTestFile(filepath.Join(testBaseDir, "subdir-2/four/five/six/b.txt"), 9))
	must(createTestFile(filepath.Join(testBaseDir, "subdir-2/seven/eight/nine/c.txt"), 25))

	config := getDefaultTestConfig()
	defer config.Cleanup()
	config.SizeThreshold = 1000
	roundTripTest(testBaseDir, config, t)
}

func TestRoundTrip_BatchingChangesAcrossRuns(t *testing.T) {
	// Create test directory and write a bunch of files
	testBaseDir, err := os.MkdirTemp("/tmp", "dave-backup-test-")
	must(err)

	defer (func() {
		must(os.RemoveAll(testBaseDir))
	})()

	// These should get grouped
	must(createTestFile(filepath.Join(testBaseDir, "a.txt"), 5))
	must(createTestFile(filepath.Join(testBaseDir, "b.txt"), 9))
	must(createTestFile(filepath.Join(testBaseDir, "c.txt"), 25))

	// These should _not_ get grouped because one of the files is too big
	must(createTestFile(filepath.Join(testBaseDir, "subdir-1/one/two/three/a.txt"), 5))
	must(createTestFile(filepath.Join(testBaseDir, "subdir-1/four/five/six/b.txt"), 9))
	must(createTestFile(filepath.Join(testBaseDir, "subdir-1/four/five/six/big.txt"), 2000))
	must(createTestFile(filepath.Join(testBaseDir, "subdir-1/seven/eight/nine/c.txt"), 25))

	// These should get grouped together separately from the top-level files
	must(createTestFile(filepath.Join(testBaseDir, "subdir-2/one/two/three/a.txt"), 5))
	must(createTestFile(filepath.Join(testBaseDir, "subdir-2/four/five/six/b.txt"), 9))
	must(createTestFile(filepath.Join(testBaseDir, "subdir-2/seven/eight/nine/c.txt"), 25))

	config := getDefaultTestConfig()
	defer config.Cleanup()
	config.SizeThreshold = 1000
	roundTripTest(testBaseDir, config, t)

	// Remove the large file down the three, causing the entire directory hierarchy to collapse into
	// one batch. Also tests that file deletion is working properly.
	must(os.Remove(filepath.Join(testBaseDir, "subdir-1/four/five/six/big.txt")))

	// Run the test again, _without_ clearing the bucket (so we effectively get the same behavior as a
	// non-fresh run in real life).
	config.LeaveBucketContents = true
	roundTripTest(testBaseDir, config, t)

	// Check manually that there is only one batch in the db
	dbFile := filepath.Join(testBaseDir, "backup.db")
	db, err := backup.NewDB(dbFile)
	must(err)
	batchesInDb, err := db.GetExistingBatches(true)
	must(err)
	if len(batchesInDb) != 1 {
		t.Fatalf("expected 1 batch in the db, got: %+v", batchesInDb)
	}

	// Check manually for extraneous S3 files. There should now only be one in this prefix, since the
	// smaller batches from the first run should have been cleaned up.
	cfg := backup.GetMinioConfig(minioUrl)
	client := s3.NewFromConfig(*cfg)
	output, err := client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(config.S3Prefix),
	})
	if err != nil {
		log.Fatal(err)
	}
	if len(output.Contents) > 1 {
		t.Fatalf("expected just 1 S3 file, got: %+v", util.Map(output.Contents, func(o types.Object) string {
			return *o.Key
		}))
	}
}

func TestRoundTrip_MultiRun(t *testing.T) {
	// Create test directory and write a bunch of files
	testBaseDir, err := os.MkdirTemp("/tmp", "dave-backup-test-")
	must(err)

	defer (func() {
		must(os.RemoveAll(testBaseDir))
	})()

	type runFileSpec struct {
		Path string
		Size int
	}
	type runSpec struct {
		Files []runFileSpec
	}
	runSpecs := []runSpec{
		{
			Files: []runFileSpec{
				{Path: "a.txt", Size: 5},
				{Path: "b.txt", Size: 9},
				{Path: "c.txt", Size: 25},
			},
		},
	}
	runSpecs = append(runSpecs, runSpec{
		Files: append(
			[]runFileSpec{
				{Path: "subdir-1/one/two/three/a.txt", Size: 5},
				{Path: "subdir-1/four/five/six/b.txt", Size: 9},
				{Path: "subdir-1/seven/eight/nine/c.txt", Size: 25},
				{Path: "subdir-2/with/many/directories/a.txt", Size: 5},
				{Path: "subdir-2/with/many/directories/b.txt", Size: 9},
				{Path: "subdir-2/with/many/directories/c.txt", Size: 25},
			},
			runSpecs[0].Files...,
		),
	})
	runSpecs = append(runSpecs, runSpec{
		Files: append(
			[]runFileSpec{},
			// Remove all files that contain "b.txt" in the name, and rezise the a.txt files to be bigger.
			util.Map(
				util.Filter(runSpecs[0].Files, func(f runFileSpec) bool {
					return !strings.Contains(f.Path, "b.txt")
				}),
				func(f runFileSpec) runFileSpec {
					if strings.Contains(f.Path, "a.txt") {
						f.Size += 100
					}
					return f
				},
			)...,
		),
	})

	config := getDefaultTestConfig()
	defer config.Cleanup()

	for i, runSpec := range runSpecs {
		for _, fileSpec := range runSpec.Files {
			must(createTestFile(filepath.Join(testBaseDir, fileSpec.Path), fileSpec.Size))
		}
		fmt.Printf("+++ running round trip test for run %d\n", i)
		roundTripTest(testBaseDir, config, t)
		fmt.Printf("--- finished round trip test for run %d\n", i)
	}
}

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
	SizeThreshold int64
	// If true, doesn't clear out the S3 bucket's contents. Useful for running multiple tests in a row
	// to validate dirty checks.
	LeaveBucketContents bool
	S3Prefix            string
	Cleanup             func()
}

func getDefaultTestConfig() *roundTripTestConfig {
	myPrefix := prefixBase + "-" + randSeq(16)

	return &roundTripTestConfig{
		SizeThreshold: 10,
		S3Prefix:      myPrefix,
		Cleanup: func() {
			cfg := backup.GetMinioConfig(minioUrl)
			client := s3.NewFromConfig(*cfg)
			must(clearBucket(client, bucket, myPrefix))
		},
	}
}

func roundTripTest(testBaseDir string, testConfig *roundTripTestConfig, t *testing.T) {
	testRecoveryDir, err := os.MkdirTemp("/tmp", "dave-recovery-test-")
	must(err)

	defer (func() {
		must(os.RemoveAll(testRecoveryDir))
	})()

	cfg := backup.GetMinioConfig(minioUrl)
	client := s3.NewFromConfig(*cfg)
	dbFile := filepath.Join(testBaseDir, "backup.db")

	if !testConfig.LeaveBucketContents {
		must(clearBucket(client, bucket, testConfig.S3Prefix))
	}

	logger := &logging.DefaultLogger{
		Level: logging.Debug,
	}

	// a and b but not c will be tarred/gzipped
	must(backup.BackupFiles(logger, cfg, dbFile, testBaseDir, bucket, testConfig.S3Prefix, testConfig.SizeThreshold, false))
	must(backup.RecoverFiles(cfg, bucket, testConfig.S3Prefix, testRecoveryDir))

	compareDirectories(testBaseDir, testRecoveryDir, t)

	// Read the backup db and find all of the S3 keys that should exist.
	db, err := backup.NewDB(dbFile)
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

	// Make sure the objects in S3 match the batches in the db.
	unexpectedBatches := make(map[string]struct{})
	for _, object := range batchesInS3.Contents {
		unexpectedBatches[*object.Key] = struct{}{}
	}
	for _, batch := range batchesInDb {
		var batchKey string
		if batch.IsSingleFile {
			batchKey = fmt.Sprintf("%s/%s.gz", testConfig.S3Prefix, batch.Path)
		} else {
			if batch.Path == "." {
				batchKey = fmt.Sprintf("%s/_files.tar.gz", testConfig.S3Prefix)
			} else {
				batchKey = fmt.Sprintf("%s/%s/_files.tar.gz", testConfig.S3Prefix, batch.Path)
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
