package main

import (
	"flag"

	"local/backup/lib/backup"
	"local/backup/lib/logging"
)

func main() {
	fMetaDb := flag.String("db", "backup.db", "database location for local cache storage")
	fRootDir := flag.String("dir", ".", "root directory for backup operation")
	fSizeThreshold := flag.Int64("size_threshold", 1024*1024, "defines the threshold above which a file gets backed up by itself, as well as the max size of a directory to get zipped together")
	fBucket := flag.String("bucket", "my-bucket", "S3 bucket")
	fPrefix := flag.String("prefix", "my-backups", "Prefix for the files stored in the S3 bucket")
	fDoRecover := flag.Bool("recover", false, "If true, recovers FROM the remote location TO the local location")
	fDryRun := flag.Bool("dry_run", true, "if true, print a plan and don't actually send any files to the backup destination")
	fLogLevel := flag.String("log_level", "info", "controls logging verbosity")
	fS3Url := flag.String("s3_url", "http://localhost:9000", "URL of S3 service")
	flag.Parse()

	cfg := backup.GetMinioConfig(*fS3Url)

	logger := &logging.DefaultLogger{
		Level: logging.Info,
	}
	switch *fLogLevel {
	case "debug":
		logger.Level = logging.Debug
	case "verbose":
		logger.Level = logging.Verbose
	}

	if *fDoRecover {
		backup.RecoverFiles(cfg, *fBucket, *fPrefix, *fRootDir)
	} else {
		backup.BackupFiles(logger, cfg, *fMetaDb, *fRootDir, *fBucket, *fPrefix, *fSizeThreshold, *fDryRun)
	}
}
