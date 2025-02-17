package backup

import (
	"crypto/md5"
	"database/sql"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"local/backup/logging"
)

// TODO: return errors rather than Fatal-ing
func BackupFiles(
	logger logging.Logger,
	cfg *aws.Config,
	dbFile string,
	localRoot string,
	bucket string,
	prefix string,
	sizeThreshold int64,
	dryRun bool,
) error {
	logger.Debugf("size threshold: %d", sizeThreshold)

	// Load the db
	db, err := newDB(dbFile)
	if err != nil {
		log.Fatalf("error loading db: %v", err)
	}

	// Create an Amazon S3 service client
	client := s3.NewFromConfig(*cfg)

	// Clean up the root path, since it was user input (e.g. resolve '..' elements).
	cleanRoot := filepath.Clean(localRoot)

	logger.Infof("> Scanning files")
	batches, err := getFilesToBackup(db, cleanRoot, sizeThreshold)
	if err != nil {
		log.Fatalf("error finding files to backup: %v", err)
	}
	logger.Infof("< Scanning files")

	logger.Verbosef("> Found files")
	for _, batch := range batches {
		logger.Verbosef("batch %s (%d bytes)", batch.Root, batch.Size())
		for _, file := range batch.Files {
			dirty := ""
			if file.IsDirty {
				dirty = "[dirty] "
			}
			logger.Verbosef("  %s%s (%d bytes)", dirty, file.Path, file.Size())
		}
	}
	logger.Verbosef("< Found files")

	if dryRun {
		logger.Infof("dry run - not copying files to backup destination")
		return nil
	}

	logger.Infof("> Backing up files")

	// TODO: check for duplicate batches by path

	for _, batch := range batches {
		err = backupBatch(logger, db, client, cleanRoot, bucket, prefix, batch)
		if err != nil {
			log.Fatalf("error backing up batch: %+v", err)
		}
	}
	logger.Infof("< Backing up files")

	return nil
}

func backupBatch(
	logger logging.Logger,
	db *DB,
	client *s3.Client,
	root string,
	bucket string,
	prefix string,
	batch *BackupBatch,
) error {
	if len(batch.Files) == 0 {
		return nil
	}

	anyDirty := false
	for _, file := range batch.Files {
		if file.IsDirty {
			anyDirty = true
		} else {
			// Check if this file has moved to a different batch (potentially due to other files changing
			// the batching structure). In this case even if the file is unchanged, we want to update it,
			// so our backup structure is fully up to date.
			changed, err := fileHasChangedBatch(db, file.Path, batch.Root)
			if err != nil {
				return fmt.Errorf("failed to check if file has changed batch %q: %v", file, err)
			}
			if changed {
				logger.Debugf("file %q has changed batches", file)
				anyDirty = true
			}
		}
	}
	if !anyDirty {
		logger.Verbosef("no dirty files in batch, skipping: %q", batch.Root)
		return nil
	}

	if len(batch.Files) > 1 {
		// Grab the batch name relative to the root directory.
		relativeRoot, err := filepath.Rel(root, batch.Root)
		if err != nil {
			return err
		}

		var files []string
		for _, file := range batch.Files {
			files = append(files, file.Path)
		}
		logger.Verbosef("Backing up file batch: %s, dirty files: %v", relativeRoot, files)

		err = backupDirectory(client, bucket, prefix, root, batch.Root, files)
		if err != nil {
			return fmt.Errorf("failed to backup batch %q: %+v", batch.Root, err)
		}
		for _, f := range files {
			// TODO: only mark files if they were dirty?
			markFile(db, f, batch.Root)
		}
	} else {
		filePath := batch.Files[0].Path
		relativePath, err := filepath.Rel(root, filePath)
		if err != nil {
			return err
		}
		logger.Verbosef("Backing up file: %s", relativePath)
		err = backupFile(client, bucket, prefix, root, filePath)
		if err != nil {
			return fmt.Errorf("failed to backup file %q: %+v", filePath, err)
		}
		// Empty string signifies that this file was not in a batch and was backed up individually
		err = markFile(db, filePath, "")
		if err != nil {
			log.Fatalf("error marking file as processed: %v", err)
		}
	}
	return nil
}

func markFile(db *DB, path string, batch string) error {
	info, err := os.Stat(path)
	if err != nil {
		log.Fatalf("error stat-ing file: %v", err)
	}
	hash, err := getFileHash(path)
	if err != nil {
		log.Fatalf("error hashing file: %v", err)
	}

	return db.MarkFile(path, info.ModTime(), hash, batch)
}

func getFileHash(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()

	h := md5.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}

	return fmt.Sprintf("%x", h.Sum(nil)), nil
}

func fileHasChangedBatch(db *DB, path string, batch string) (bool, error) {
	fi, err := db.GetFileInfo(path)
	if err != nil && err != sql.ErrNoRows {
		return false, err
	}

	if err == sql.ErrNoRows {
		// If we have no record of the file, say it's changed batches (shouldn't matter because in the
		// calling code we'll also mark the file as new).
		return true, nil
	}

	return batch != fi.Batch, nil
}

func doesFileNeedBackup(db *DB, path string, info fs.FileInfo) (bool, error) {
	fi, err := db.GetFileInfo(path)
	if err != nil && err != sql.ErrNoRows {
		return false, err
	}

	if err == sql.ErrNoRows {
		return true, nil
	}

	modTimeChanged := !info.ModTime().Truncate(time.Millisecond).Equal(fi.ModTime.Truncate(time.Millisecond))

	hash, err := getFileHash(path)
	if err != nil {
		return false, err
	}
	hashChanged := hash != fi.Hash

	// Only hold off updating the file if:
	//   - Its mod time is the same
	//   - Its hash is the same
	// Otherwise default to uploading the file, because we'd rather send some duplicative data than
	// not back up a file.
	if !modTimeChanged && !hashChanged {
		return false, nil
	}
	return true, nil
}

type QueueItem struct {
	Path  string
	Depth int
}

type BackupRequest struct {
	Path             string
	IsDir            bool
	Recurse          bool
	DirtySubFiles    []string
	SubFilesToBackup []string
}

func doBackupFile(path string) bool {
	filename := filepath.Base(path)

	// TODO: this check is kinda janky
	if filename == "backup.db" {
		return false
	}
	return true
}

type BackupFile struct {
	FileSize int64
	Path     string
	IsDirty  bool
}

func (b *BackupFile) Size() int64 {
	return b.FileSize
}

type BackupBatch struct {
	// Root directory that the files should be relative to (i.e. where the zip file should be
	// produced). Ignored for single-file batches.
	Root      string
	TotalSize int64
	Files     []*BackupFile
}

func (b *BackupBatch) Size() int64 {
	return b.TotalSize
}

type WithSize interface {
	Size() int64
}

func sumSizes[T WithSize](arr []T) int64 {
	sum := int64(0)
	for _, e := range arr {
		sum += e.Size()
	}
	return sum
}

// Depth-first search into this directory tree.
// For each directory, determine the total size of all files in the tree.
// If the sum is greater than the max, remove the largest subdirectory (mark it as to-be-zipped),
// then repeat until we are below the max.
func getFilesToBackup(db *DB, root string, sizeThreshold int64) ([]*BackupBatch, error) {
	// TODO: max depth check

	// Get files in directory
	files, err := os.ReadDir(root)
	if err != nil {
		return nil, fmt.Errorf("error scanning directory: %v", err)
	}

	var dirFiles []*BackupFile
	var maybeRollupBatches []*BackupBatch
	var otherBatches []*BackupBatch
	for _, file := range files {
		path := filepath.Join(root, file.Name())
		log.Printf("scanning path %q", path)

		if file.IsDir() {
			subBatches, err := getFilesToBackup(db, path, sizeThreshold)
			if err != nil {
				return nil, err
			}
			if len(subBatches) > 1 {
				otherBatches = append(otherBatches, subBatches...)
			} else {
				maybeRollupBatches = append(maybeRollupBatches, subBatches...)
			}
		} else {
			if !doBackupFile(path) {
				continue
			}
			info, err := file.Info()
			if err != nil {
				log.Fatal(err)
			}
			isDirty, err := doesFileNeedBackup(db, path, info)
			if err != nil {
				log.Fatal(err)
			}
			dirFiles = append(dirFiles, &BackupFile{
				Path:     path,
				FileSize: info.Size(),
				IsDirty:  isDirty,
			})
			log.Printf("  found file %q", path)
		}
	}

	var outputBatches []*BackupBatch

	// Start by rolling up the files at the current directory's level.
	if len(dirFiles) > 0 {
		sum := sumSizes(dirFiles)
		if sum <= sizeThreshold {
			// Just send them all as a zip file
			outputBatches = append(outputBatches, &BackupBatch{
				Root:      root,
				TotalSize: sum,
				Files:     dirFiles,
			})
		} else {
			// Sort files by size descending
			sort.Slice(dirFiles, func(i, j int) bool {
				// Intentionally use > so the sort is reversed
				return dirFiles[i].Size() > dirFiles[j].Size()
			})
			// Pop individual files off the stack until we find one that's below
			// the limit, then send all the rest in a zip file.
			for sum > sizeThreshold && len(dirFiles) > 0 {
				outputBatches = append(outputBatches, &BackupBatch{
					Files:     []*BackupFile{dirFiles[0]},
					TotalSize: dirFiles[0].Size(),
				})
				sum -= dirFiles[0].Size()
				dirFiles = dirFiles[1:]
			}
			// Add the remaining files as a batch
			outputBatches = append(outputBatches, &BackupBatch{
				Root:      root,
				Files:     dirFiles,
				TotalSize: sum,
			})
		}
	}

	if
	// If there's only one (or no) output batch so far, then we're able to zip up all the files and
	// still be under the threshold. Check if we can also roll the subdirectories in.
	len(outputBatches) <= 1 &&
		// If there are no sub-batches to roll up, don't bother.
		len(maybeRollupBatches) > 0 &&
		// This means none of the subdirectories was large enough to split it up, so the entire tree
		// is below the size threshold. Attempt to roll up all subdirectories along with the files at
		// the current directory level.
		len(otherBatches) == 0 {
		totalSize := sumSizes(maybeRollupBatches)
		if len(outputBatches) > 0 {
			outputBatches[0].Size()
		}
		if totalSize <= sizeThreshold {
			// If the total is still below the threshold, jam everything into one big batch.
			var allFiles []*BackupFile
			if len(outputBatches) > 0 {
				allFiles = outputBatches[0].Files
			}
			for _, batch := range maybeRollupBatches {
				allFiles = append(allFiles, batch.Files...)
			}
			outputBatches = []*BackupBatch{
				{
					Root:      root,
					Files:     allFiles,
					TotalSize: totalSize,
				},
			}
			return outputBatches, nil
		}
	}

	// If we've gotten this far, then the tree at this directory level and below is larger than the
	// threshold, so we need to pass up the batches from all subdirectories as is.
	outputBatches = append(outputBatches, otherBatches...)
	outputBatches = append(outputBatches, maybeRollupBatches...)

	return outputBatches, nil
}
