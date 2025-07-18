package main

import (
	"crypto/md5"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"local/backup/lib/backup"
	"local/backup/lib/logging"

	"github.com/aws/aws-sdk-go-v2/aws"
)

func main() {
	fMetaDbDir := flag.String("db", "", "database directory for local cache storage (if not provided, will be stored in ~/.dbackup/)")
	fBackupName := flag.String("name", "", "name of the backup (if not provided, will be derived from the root directory)")
	fRootDir := flag.String("dir", ".", "root directory for backup operation")
	fSizeThreshold := flag.Int64("size_threshold", 1024*1024, "defines the threshold above which a file gets backed up by itself, as well as the max size of a directory to get zipped together")
	// TODO: default value
	fBucket := flag.String("bucket", "my-bucket", "S3 bucket")
	fPrefix := flag.String("prefix", "backups", "Custom prefix for the files stored in the S3 bucket")
	fDoRecover := flag.Bool("recover", false, "If true, recovers FROM the remote location TO the local location")
	fDryRun := flag.Bool("dry_run", true, "if true, print a plan and don't actually send any files to the backup destination")
	fLogLevel := flag.String("log_level", "info", "controls logging verbosity")
	fS3Url := flag.String("s3_url", "http://localhost:9000", "URL of S3 service")
	fForce := flag.Bool("force", false, "if true, will overwrite any existing files in the remote backup regardless of the check")
	flag.Parse()

	var cfg *aws.Config
	// Default to minio so we don't accidentally blow away any real backups while testing
	if *fS3Url != "" {
		cfg = backup.GetMinioConfig(*fS3Url)
	} else {
		cfg = backup.GetS3Config()
	}

	logger := &logging.DefaultLogger{
		Level: logging.Info,
	}
	switch *fLogLevel {
	case "debug":
		logger.Level = logging.Debug
	case "verbose":
		logger.Level = logging.Verbose
	case "info":
		logger.Level = logging.Info
	}

	backupName := *fBackupName
	if backupName == "" {
		// MD5 hash of the normalized absolute root directory
		absRootDir, err := filepath.Abs(*fRootDir)
		if err != nil {
			log.Fatal(err)
		}
		hashBs := md5.Sum([]byte(filepath.Clean(absRootDir)))
		backupName = fmt.Sprintf("%x", hashBs)
	}

	bucket := *fBucket

	dbDir := *fMetaDbDir
	if dbDir == "" {
		homeDir, err := os.UserHomeDir()
		if err != nil {
			log.Fatal(err)
		}
		dbDir = filepath.Join(homeDir, ".dbackup")
	}

	dbFile := filepath.Join(dbDir, fmt.Sprintf("%s.db", backupName))
	absDbFile, err := filepath.Abs(dbFile)
	if err != nil {
		log.Fatal(err)
	}
	dbFile = filepath.Clean(absDbFile)
	logger.Infof("using db file: %s", dbFile)

	if *fDoRecover {
		err := backup.RecoverFiles(
			logger,
			cfg,
			dbFile,
			bucket,
			*fPrefix,
			backupName,
			*fRootDir,
			backup.RecoveryOptions{
				Force: *fForce,
			},
		)
		if err != nil {
			log.Fatalf("error recovering files: %+v", err)
		}
	} else {
		err := backup.BackupFiles(
			logger,
			cfg,
			dbFile,
			*fRootDir,
			bucket,
			*fPrefix,
			backupName,
			*fSizeThreshold,
			backup.BackupOptions{
				DryRun: *fDryRun,
				Force:  *fForce,
			},
		)
		if err != nil {
			log.Fatalf("error backing up files: %+v", err)
		}
	}
}
