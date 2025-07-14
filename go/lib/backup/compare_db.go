package backup

import (
	"errors"
	"fmt"
	"local/backup/lib/logging"
	"local/backup/lib/s3_helpers"
	"os"
	"path/filepath"
	"sort"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func backupDB(logger logging.Logger, client *s3.Client, dbFile string, bucket string, prefix string) error {
	dir := filepath.Dir(dbFile)
	file := filepath.Base(dbFile)

	// Explicitly don't use the archive, since changing the modtime of an SQLite database is
	// potentially dangerous.
	return backupFileNoArchive(logger, client, bucket, prefix, dir, file)
	//return backupFile(logger, client, bucket, prefix, dir, file)
}

func downloadAndCompareDB(
	logger logging.Logger,
	client *s3.Client,
	dbFile string,
	bucket string,
	prefixBase string,
	backupName string,
) ([]string, error) {
	// Check if the local db exists. If not, then we're doing a fresh backup or recovery.
	if _, err := os.Stat(dbFile); os.IsNotExist(err) {
		return nil, nil
	}

	remoteDBFile, err := downloadDB(logger, client, bucket, prefixBase, backupName, os.TempDir())
	if err != nil {
		if errors.Is(err, s3_helpers.ErrNotFound) {
			// This just means the backup doesn't exist yet.
			return nil, nil
		}
		return nil, err
	}
	defer os.Remove(remoteDBFile)
	logger.Verbosef("downloaded remote db file to %q", remoteDBFile)

	logger.Verbosef("comparing db file %q with remote db file %q", dbFile, remoteDBFile)

	localDb, err := NewDB(dbFile)
	if err != nil {
		return nil, fmt.Errorf("failed to open db file %q: %v", dbFile, err)
	}
	defer localDb.Close()

	remoteDb, err := NewDB(remoteDBFile)
	if err != nil {
		return nil, fmt.Errorf("failed to open db file %q: %v", remoteDBFile, err)
	}
	defer remoteDb.Close()

	// Load all files from the local and remote dbs.
	localFiles, err := localDb.GetAllFiles()
	if err != nil {
		return nil, fmt.Errorf("failed to get all files from local db: %v", err)
	}
	remoteFiles, err := remoteDb.GetAllFiles()
	if err != nil {
		return nil, fmt.Errorf("failed to get all files from remote db: %v", err)
	}

	// Sort each list by path.
	sort.Slice(localFiles, func(i, j int) bool {
		return localFiles[i].Path < localFiles[j].Path
	})
	sort.Slice(remoteFiles, func(i, j int) bool {
		return remoteFiles[i].Path < remoteFiles[j].Path
	})

	// Put each list in a map by path.
	localFilesMap := make(map[string]*FileInfo)
	for _, file := range localFiles {
		localFilesMap[file.Path] = file
	}
	remoteFilesMap := make(map[string]*FileInfo)
	for _, file := range remoteFiles {
		remoteFilesMap[file.Path] = file
	}

	var changes []string

	// Compare each file in the local db to the remote db.
	for _, localFile := range localFiles {
		remoteFile, ok := remoteFilesMap[localFile.Path]
		if !ok {
			changes = append(changes, fmt.Sprintf("%q not found in remote db", localFile.Path))
			continue
		}
		if localFile.ModTime != remoteFile.ModTime {
			changes = append(changes, fmt.Sprintf("%q has different mod time in local and remote db", localFile.Path))
		}
		if localFile.Hash != remoteFile.Hash {
			changes = append(changes, fmt.Sprintf("%q has different hash in local and remote db", localFile.Path))
		}
		if localFile.Batch != remoteFile.Batch {
			changes = append(changes, fmt.Sprintf("%q has different batch in local and remote db", localFile.Path))
		}
	}

	// Finally, check for files that are in the remote db but not the local db.
	for _, remoteFile := range remoteFiles {
		if _, ok := localFilesMap[remoteFile.Path]; !ok {
			changes = append(changes, fmt.Sprintf("%q not found in local db", remoteFile.Path))
		}
	}

	return changes, nil
}

func downloadDB(
	logger logging.Logger,
	client *s3.Client,
	bucket string,
	prefixBase string,
	backupName string,
	localDir string,
) (string, error) {
	// Download the remote DB file.
	remoteDBKey := filepath.Join(prefixBase, fmt.Sprintf("%s.db.gz", backupName))
	remoteDBFileCompressed := filepath.Join(localDir, fmt.Sprintf("%s.db.gz", backupName))
	logger.Verbosef("downloading db from %q to %q", remoteDBKey, remoteDBFileCompressed)
	err := s3_helpers.DownloadFile(client, bucket, remoteDBKey, remoteDBFileCompressed)
	if err != nil {
		return "", err
	}
	defer os.Remove(remoteDBFileCompressed)

	// Decompress the remote db file.
	remoteDBFile, err := decompressFile(remoteDBFileCompressed, localDir)
	if err != nil {
		return "", fmt.Errorf("failed to decompress db file: %v", err)
	}

	//err = unTar(remoteDBFileCompressed, localDir)
	//if err != nil {
	//	return "", fmt.Errorf("failed to untar db file: %v", err)
	//}
	//remoteDBFile := strings.TrimSuffix(remoteDBFileCompressed, ".tar.gz")

	return remoteDBFile, nil
}

func printChanges(changes []string) {
	for _, change := range changes {
		fmt.Println(change)
	}
}
