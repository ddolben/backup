package main

import (
	"flag"

	"local/backup/backup"
)

func main() {
	fMetaDb := flag.String("db", "backup.db", "database location for local cache storage")
	fRootDir := flag.String("dir", ".", "root directory for backup operation")
	fSizeThreshold := flag.Int64("size_threshold", 1000000, "defines the threshold above which a file gets backed up by itself, as well as the max size of a directory to get zipped together")
	fBucket := flag.String("bucket", "test-bucket", "S3 bucket")
	fDoRecover := flag.Bool("recover", false, "If true, recovers FROM the remote location TO the local location")
	fDryRun := flag.Bool("dry_run", true, "if true, print a plan and don't actually send any files to the backup destination")
	flag.Parse()

	cfg := backup.GetMinioConfig("http://localhost:9000")

	if *fDoRecover {
		backup.RecoverFiles(cfg, *fBucket, *fRootDir)
	} else {
		backup.BackupFiles(cfg, *fMetaDb, *fRootDir, *fBucket, *fSizeThreshold, *fDryRun)
	}
}
