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

func downloadAndCompareDB(
	logger logging.Logger,
	client *s3.Client,
	dbFile string,
	bucket string,
	prefixBase string,
	backupName string,
) ([]string, error) {
	remoteDBFileCompressed, err := downloadDB(client, bucket, prefixBase, backupName)
	if err != nil {
		if errors.Is(err, s3_helpers.ErrNotFound) {
			// This just means the backup doesn't exist yet.
			return nil, nil
		}
		return nil, err
	}
	defer os.Remove(remoteDBFileCompressed)

	// Decompress the remote db file.
	remoteDBFile, err := decompressFile(remoteDBFileCompressed, os.TempDir())
	if err != nil {
		return nil, fmt.Errorf("failed to decompress db file: %v", err)
	}
	defer os.Remove(remoteDBFile)

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
	client *s3.Client,
	bucket string,
	prefixBase string,
	backupName string,
) (string, error) {
	remoteDBKey := filepath.Join(prefixBase, fmt.Sprintf("%s.db.gz", backupName))
	remoteDBFile := filepath.Join(os.TempDir(), fmt.Sprintf("%s.db.gz", backupName))
	err := s3_helpers.DownloadFile(client, bucket, remoteDBKey, remoteDBFile)
	if err != nil {
		return "", err
	}
	return remoteDBFile, nil
}
