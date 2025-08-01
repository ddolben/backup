package backup

import (
	"context"
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
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"local/backup/lib/logging"
	"local/backup/lib/util"
)

type BackupOptions struct {
	Force  bool
	DryRun bool
}

// TODO: return errors rather than Fatal-ing
// TODO: options argument (with validation)
func BackupFiles(
	logger logging.Logger,
	cfg *aws.Config,
	dbFile string,
	localRoot string,
	bucket string,
	prefixBase string,
	name string,
	sizeThreshold int64,
	options BackupOptions,
) error {
	prefix := filepath.Join(prefixBase, name)
	logger.Infof("using s3 prefix: s3://%s/%s", bucket, prefix)

	logger.Debugf("size threshold: %d", sizeThreshold)

	// Load the db
	db, err := NewDB(dbFile)
	if err != nil {
		log.Fatalf("error loading db: %v", err)
	}

	// Create an Amazon S3 service client
	client := s3.NewFromConfig(*cfg)

	// Clean up the root path, since it was user input (e.g. resolve '..' elements).
	cleanRoot := filepath.Clean(localRoot)

	// Download the backup db from S3 and check if any files have changed since the last time we did a
	// backup.
	changes, err := downloadAndCompareDB(logger, client, dbFile, bucket, prefixBase, name)
	if err != nil {
		log.Fatalf("error downloading and comparing db: %v", err)
	}
	if len(changes) > 0 {
		logger.Infof("files have changed in storage since the last backup, aborting:")
		printChanges(changes)
		if options.Force {
			logger.Infof("forcing backup despite changes in storage")
		} else {
			return fmt.Errorf("files have changed in storage since the last backup")
		}
	}

	summary := &backupSummary{}

	// Scan through all the files in the directory and arrange them into batches.
	logger.Verbosef("> Scanning files")
	batches, err := getFilesToBackup(logger, db, cleanRoot, cleanRoot, sizeThreshold, summary)
	if err != nil {
		log.Fatalf("error finding files to backup: %v", err)
	}
	batchesToDelete, err := getBatchesToDelete(db, batches)
	if err != nil {
		log.Fatalf("error finding batches to delete: %v", err)
	}
	logger.Verbosef("< Scanning files")

	// Diff the list of files in the db with the list of files in the directory
	deletedFiles, err := getFilesNotInBatches(db, batches)
	if err != nil {
		log.Fatalf("error getting files in db: %v", err)
	}
	for _, file := range deletedFiles {
		summary.AddFile(file, backupOpRemove)
	}
	// Print the summary
	summary.Print(logger)

	// Log the batches for debugging
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
	logger.Verbosef(("batches to delete:"))
	for _, batch := range batchesToDelete {
		logger.Verbosef("  %s (%b)", batch.Path, batch.IsSingleFile)
	}
	logger.Verbosef("< Found files")

	logger.Verbosef("> Backing up files")

	logger.Debugf("Bucket: %s", bucket)
	// Make sure the bucket exists
	_, err = client.HeadBucket(context.TODO(), &s3.HeadBucketInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		log.Fatalf("bucket doesn't exist: %v", err)
	}
	logger.Debugf("Bucket exists")

	// TODO: check for duplicate batches by path

	// Delete any batches in the existing backup that no longer exist. Do this first as a precaution
	// so we don't accidentally delete files that should still be in the backup.
	logger.Verbosef(">> Clearing unnecessary batches")
	for _, batch := range batchesToDelete {
		err = deleteBatch(logger, db, client, cleanRoot, bucket, prefix, batch, options.DryRun)
		if err != nil {
			log.Fatalf("error deleting batch: %+v", err)
		}
	}
	logger.Verbosef("<< Clearing unnecessary batches")

	// Backup all batches that have dirty files
	logger.Verbosef(">> Backing up batches")
	for _, batch := range batches {
		err = backupBatch(logger, db, client, cleanRoot, bucket, prefix, batch, options.DryRun)
		if err != nil {
			log.Fatalf("error backing up batch: %+v", err)
		}
	}
	logger.Verbosef("<< Backing up batches")
	logger.Verbosef("< Backing up files")

	// Back up the DB file to the S3 prefix
	if !options.DryRun {
		logger.Verbosef("> Backing up db")
		err = backupDB(logger, client, dbFile, bucket, prefixBase)
		if err != nil {
			log.Fatalf("error backing up db: %+v", err)
		}
		logger.Verbosef("< Backing up db")
	}

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
	dryRun bool,
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
			changed, err := fileHasChangedBatch(db, root, file.Path, batch.Root)
			if err != nil {
				return fmt.Errorf("failed to check if file has changed batch %q: %v", file.Path, err)
			}
			if changed {
				logger.Debugf("file %q has changed batches", file.Path)
				anyDirty = true
			}
		}
	}
	if !anyDirty {
		logger.Verbosef("no dirty files in batch, skipping: %q", batch.Root)
		return nil
	}

	if dryRun {
		logger.Infof("dry run, would have backed up batch %q, files:", batch.Root)
		for _, file := range batch.Files {
			logger.Infof("  %s", file.Path)
		}
		return nil
	}

	if len(batch.Files) > 1 {
		var files []string
		for _, file := range batch.Files {
			files = append(files, file.Path)
		}
		logger.Verbosef("Backing up file batch: %s, dirty files: %v", batch.Root, files)

		err := backupDirectory(logger, client, bucket, prefix, root, batch.Root, files)
		if err != nil {
			return fmt.Errorf("failed to backup batch %q: %+v", batch.Root, err)
		}
		for _, f := range files {
			// TODO: only mark files if they were dirty?
			markFile(db, root, f, batch.Root)
		}
	} else {
		logger.Verbosef("Backing up file: %s", batch.Root)
		filePath := batch.Files[0].Path
		err := backupFile(logger, client, bucket, prefix, root, filePath)
		if err != nil {
			return fmt.Errorf("failed to backup file %q: %+v", filePath, err)
		}
		// Root == file path signifies that this file was not in a batch and was backed up individually
		err = markFile(db, root, filePath, filePath)
		if err != nil {
			log.Fatalf("error marking file as processed: %v", err)
		}
	}
	return nil
}

func deleteBatch(
	logger logging.Logger,
	db *DB,
	client *s3.Client,
	root string,
	bucket string,
	prefix string,
	batch BatchMeta,
	dryRun bool,
) error {
	keyPath := filepath.Join(prefix, batch.Path)
	if batch.IsSingleFile {
		keyPath = keyPath + ".tar.gz"
	} else {
		// If it's a directory, delete the archive
		keyPath = filepath.Join(keyPath, "_files.tar.gz")
	}

	if dryRun {
		logger.Infof("dry run, would have deleted S3 file %q", keyPath)
		return nil
	}

	logger.Debugf("deleting S3 file %q", keyPath)

	_, err := client.DeleteObjects(context.TODO(), &s3.DeleteObjectsInput{
		Bucket: aws.String(bucket),
		Delete: &types.Delete{
			Objects: []types.ObjectIdentifier{
				{
					Key: aws.String(keyPath),
				},
			},
		},
	})
	if err != nil {
		return err
	}

	logger.Debugf("deleting batch from db: %q", batch.Path)
	files, err := db.GetFilesInBatch(batch.Path)
	if err != nil {
		return fmt.Errorf("error getting files in batch: %v", err)
	}
	for _, file := range files {
		logger.Debugf("  %s", file)
	}

	// Delete the batch from the db
	err = db.DeleteBatch(batch.Path)
	if err != nil {
		return fmt.Errorf("error deleting batch from db: %v", err)
	}

	return nil
}

func markFile(db *DB, localRoot string, path string, batch string) error {
	absolutePath := filepath.Join(localRoot, path)
	info, err := os.Stat(absolutePath)
	if err != nil {
		return util.ErrorOrPanic("error stat-ing file: %v", err)
	}
	hash, err := getFileHash(absolutePath)
	if err != nil {
		return util.ErrorOrPanic("error hashing file: %v", err)
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

func fileHasChangedBatch(db *DB, root string, path string, batch string) (bool, error) {
	absolutePath := filepath.Join(root, path)
	fi, err := db.GetFileInfo(absolutePath)
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

func doesFileNeedBackup(db *DB, path string, info fs.FileInfo) (bool, backupOp, backupReason, error) {
	fi, err := db.GetFileInfo(path)
	if err != nil && err != sql.ErrNoRows {
		return false, backupOpNone, backupReasonNone, err
	}

	if err == sql.ErrNoRows {
		return true, backupOpAdd, backupReasonNew, nil
	}

	// As far as I can tell, modtimes are only guaranteed to be equal to the second.
	modTimeChanged := !info.ModTime().Truncate(time.Second).Equal(fi.ModTime.Truncate(time.Second))

	hash, err := getFileHash(path)
	if err != nil {
		return false, backupOpNone, backupReasonNone, err
	}
	hashChanged := hash != fi.Hash

	// Only hold off updating the file if:
	//   - Its mod time is the same
	//   - Its hash is the same
	// Otherwise default to uploading the file, because we'd rather send some duplicative data than
	// not back up a file.
	if !modTimeChanged && !hashChanged {
		return false, backupOpNone, backupReasonNone, nil
	}

	// If the hash changed, we know the modtime probably also changed, so output hash as the reason.
	if hashChanged {
		return true, backupOpChange, backupReasonHash, nil
	}
	log.Printf("file %q has changed modtime: %v -> %v", path, fi.ModTime, info.ModTime())
	return true, backupOpChange, backupReasonModtime, nil
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
	// For multi-file batches, the root directory that the files should be relative to (i.e. where the
	// zip file should be produced). For single-file batches, it's just the filename.
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
func getFilesToBackup(
	logger logging.Logger,
	db *DB,
	root string,
	searchPath string,
	sizeThreshold int64,
	summary *backupSummary,
) ([]*BackupBatch, error) {
	// Get files in directory
	files, err := os.ReadDir(searchPath)
	if err != nil {
		return nil, fmt.Errorf("error scanning directory: %v", err)
	}

	var dirFiles []*BackupFile
	var maybeRollupBatches []*BackupBatch
	var otherBatches []*BackupBatch
	for _, file := range files {
		path := filepath.Join(searchPath, file.Name())
		logger.Verbosef("scanning path %q", path)

		if file.IsDir() {
			subBatches, err := getFilesToBackup(logger, db, root, path, sizeThreshold, summary)
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
			isDirty, op, reason, err := doesFileNeedBackup(db, path, info)
			if err != nil {
				log.Fatal(err)
			}
			// Use relative paths for the files in the batch.
			relPath, err := filepath.Rel(root, path)
			if err != nil {
				log.Fatal(err)
			}
			summary.AddFile(path, op)
			dirFiles = append(dirFiles, &BackupFile{
				Path:     relPath,
				FileSize: info.Size(),
				IsDirty:  isDirty,
			})
			logger.Verbosef("  found file %q (dirty op: %d, reason: %d)", path, op, reason)
		}
	}

	// Special case: if there's only one batch from the lower subdirectories, bubble it up directly
	if len(dirFiles) == 0 && len(otherBatches) == 0 && len(maybeRollupBatches) == 1 {
		logger.Verbosef("special case for batch %q", maybeRollupBatches[0].Root)
		return maybeRollupBatches, nil
	}

	var outputBatches []*BackupBatch

	// Use the path relative to the directory root as the batch name
	relativeRoot, err := filepath.Rel(root, searchPath)
	if err != nil {
		return nil, fmt.Errorf("failed to get relative path: %v", err)
	}

	// Start by rolling up the files at the current directory's level.
	if len(dirFiles) > 0 {
		sum := sumSizes(dirFiles)
		if sum <= sizeThreshold {
			// Just send them all as a zip file
			// If it's just one file, use the file path as the Root.
			batchRoot := relativeRoot
			if len(dirFiles) == 1 {
				batchRoot = dirFiles[0].Path
			}
			outputBatches = append(outputBatches, &BackupBatch{
				Root:      batchRoot,
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
				relativePath := dirFiles[0].Path
				outputBatches = append(outputBatches, &BackupBatch{
					Root:      relativePath,
					Files:     []*BackupFile{dirFiles[0]},
					TotalSize: dirFiles[0].Size(),
				})
				sum -= dirFiles[0].Size()
				dirFiles = dirFiles[1:]
			}
			if len(dirFiles) > 0 {
				// Add the remaining files as a batch, if there are any.
				// If it's just one file, use the file path as the Root.
				batchRoot := relativeRoot
				if len(dirFiles) == 1 {
					batchRoot = dirFiles[0].Path
				}
				outputBatches = append(outputBatches, &BackupBatch{
					Root:      batchRoot,
					Files:     dirFiles,
					TotalSize: sum,
				})
			}
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
			totalSize += outputBatches[0].Size()
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
					Root:      relativeRoot,
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

func getBatchesToDelete(db *DB, batches []*BackupBatch) ([]BatchMeta, error) {
	// Find all batches in the backup plan (dirty or not)
	var plannedBatches []string
	for _, batch := range batches {
		plannedBatches = append(plannedBatches, batch.Root)
	}

	// Find all batches currently in the backup (scan of the db)
	existingBatches, err := db.GetExistingBatches(false)
	if err != nil {
		return nil, fmt.Errorf("error fetching existing batches from db: %v", err)
	}

	// Find all batches in the backup but not the backup plan
	existingBatchSet := make(map[string]BatchMeta)
	for _, b := range existingBatches {
		existingBatchSet[b.Path] = b
	}
	for _, b := range plannedBatches {
		delete(existingBatchSet, b)
	}
	var batchesToDelete []BatchMeta
	for _, b := range existingBatchSet {
		batchesToDelete = append(batchesToDelete, b)
	}

	return batchesToDelete, nil
}

func getFilesNotInBatches(db *DB, batches []*BackupBatch) ([]string, error) {
	// Find all files in the backup plan (dirty or not)
	filesInBatches := make(map[string]struct{})
	for _, batch := range batches {
		for _, file := range batch.Files {
			filesInBatches[file.Path] = struct{}{}
		}
	}

	// Find all files in the db
	dbFiles, err := db.GetAllFiles()
	if err != nil {
		return nil, fmt.Errorf("error getting files in db: %v", err)
	}

	// Find all files in the db that are not in the backup plan
	var filesNotInBatches []string
	for _, file := range dbFiles {
		if _, ok := filesInBatches[file.Path]; !ok {
			filesNotInBatches = append(filesNotInBatches, file.Path)
		}
	}
	return filesNotInBatches, nil
}
