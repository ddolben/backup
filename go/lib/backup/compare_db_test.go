package backup

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"

	"local/backup/lib/logging"
	"local/backup/lib/util"
)

func TestCompareDB(t *testing.T) {
	logger := &logging.DefaultLogger{
		Level: logging.Debug,
	}
	testConfig := getDefaultTestConfig()
	defer testConfig.Cleanup()
	testBaseDir := testConfig.TestBaseDir

	must(createTestFile(filepath.Join(testBaseDir, "a.txt"), 5))
	must(createTestFile(filepath.Join(testBaseDir, "b.txt"), 9))
	must(createTestFile(filepath.Join(testBaseDir, "c.txt"), 25))

	cfg := GetMinioConfig(minioUrl)
	s3Client := s3.NewFromConfig(*cfg)

	must(BackupFiles(
		logger,
		cfg,
		testConfig.DBFile,
		testConfig.TestBaseDir,
		testConfig.Bucket,
		testConfig.S3Prefix,
		testConfig.BackupName,
		testConfig.SizeThreshold,
		BackupOptions{},
	))

	testCases := []struct {
		name    string
		prepare func(db *DB)
	}{
		{
			name: "add a file",
			prepare: func(db *DB) {
				must(db.MarkFile("d.txt", time.Now(), "d", "d"))
			},
		},
		{
			name: "delete a file",
			prepare: func(db *DB) {
				must(db.DeleteFile("a.txt"))
			},
		},
		{
			name: "modify a file's modtime",
			prepare: func(db *DB) {
				filename := "a.txt"
				fi, err := db.GetFileInfo(filename)
				must(err)
				must(db.MarkFile(filename, fi.ModTime.Add(time.Second), fi.Hash, fi.Batch))
			},
		},
		{
			name: "modify a file's hash",
			prepare: func(db *DB) {
				filename := "a.txt"
				fi, err := db.GetFileInfo(filename)
				must(err)
				must(db.MarkFile(filename, fi.ModTime, "new-hash", fi.Batch))
			},
		},
		{
			name: "modify a file's path",
			prepare: func(db *DB) {
				filename := "a.txt"
				fi, err := db.GetFileInfo(filename)
				must(err)
				must(db.MarkFile(filename, fi.ModTime, fi.Hash, "new-batch"))
			},
		},
	}

	// Back up the local DB so we can recover it before each test.
	backupDBFile := testConfig.DBFile + ".backup"
	must(util.CopyFile(testConfig.DBFile, backupDBFile))
	defer os.Remove(backupDBFile)

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			// Recover the DB before the test.
			must(util.CopyFile(backupDBFile, testConfig.DBFile))

			db, err := NewDB(testConfig.DBFile)
			must(err)
			defer db.Close()
			testCase.prepare(db)

			changes, err := downloadAndCompareDB(
				logger,
				s3Client,
				testConfig.DBFile,
				testConfig.Bucket,
				testConfig.S3Prefix,
				testConfig.BackupName,
			)
			must(err)
			if len(changes) == 0 {
				t.Errorf("expected changes, got none")
			}
		})
	}
}
