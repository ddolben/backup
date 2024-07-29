package main

import (
	"flag"

	"local/backup/backup"
)

func main() {
	fMetaDb := flag.String("db", "backup.db", "database location for local cache storage")
	fRootDir := flag.String("dir", ".", "root directory for backup operation")
	fMaxDepth := flag.Int("max_depth", -1, "max subfolder depth to recurse into before just archiving the whole tree (-1 means no max)")
	fBucket := flag.String("bucket", "test-bucket", "S3 bucket")
	fIndividualSizeThreshold := flag.Int("size_threshold", 0, "size threshold, in bytes, above which files get backed up individually")
	fDoRecover := flag.Bool("recover", false, "If true, recovers FROM the remote location TO the local location")
	flag.Parse()

	cfg := backup.GetMinioConfig("http://localhost:9000")

	if *fDoRecover {
		backup.RecoverFiles(cfg, *fBucket, *fRootDir)
	} else {
		backup.BackupFiles(cfg, *fMetaDb, *fRootDir, *fBucket, *fMaxDepth, *fIndividualSizeThreshold)
	}
}
