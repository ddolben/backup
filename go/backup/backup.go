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
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// TODO: return errors rather than Fatal-ing
func BackupFiles(cfg *aws.Config, dbFile string, localRoot string, bucket string, maxDepth int, fileSizeThreshold int) error {
	// Load the db
	db, err := newDB(dbFile)
	if err != nil {
		log.Fatalf("error loading db: %v", err)
	}

	// Create an Amazon S3 service client
	client := s3.NewFromConfig(*cfg)

	log.Println("> Scanning files")
	paths, err := getFilesToBackup(db, localRoot, maxDepth, fileSizeThreshold)
	if err != nil {
		log.Fatalf("error finding files to backup: %v", err)
	}
	log.Println("< Scanning files")

	log.Println("> FOUND FILES")
	for _, path := range paths {
		if path.IsDir {
			log.Printf("d %s", path.Path)
			for _, file := range path.DirtySubFiles {
				log.Printf("  %s", file)
			}
		} else {
			log.Printf("f %s", path.Path)
		}
	}
	log.Println("< FOUND FILES")

	log.Println("> Backing up files")
	for _, path := range paths {
		// TODO: strip path so it's relative to the root
		if path.IsDir {
			log.Printf("Backing up directory: %s, dirty files:", path.Path)
			backupDirectory(client, bucket, localRoot, path.Path, path.SubFilesToBackup)
			for _, f := range path.DirtySubFiles {
				markFile(db, f)
			}
			continue
		}

		log.Printf("Backing up file: %s", path.Path)
		// TODO: strip path so it's relative to the root
		err = backupFile(client, bucket, localRoot, path.Path)
		if err != nil {
			return fmt.Errorf("failed to backup file %q: %+v", path.Path, err)
		}
		err = markFile(db, path.Path)
		if err != nil {
			log.Fatalf("error marking file as processed: %v", err)
		}
	}
	log.Println("< Backing up files")

	return nil
}

func markFile(db *DB, path string) error {
	info, err := os.Stat(path)
	if err != nil {
		log.Fatalf("error stat-ing file: %v", err)
	}
	hash, err := getFileHash(path)
	if err != nil {
		log.Fatalf("error hashing file: %v", err)
	}

	return db.MarkFile(path, info.ModTime(), hash)
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

func doesFileNeedBackup(db *DB, path string, info fs.FileInfo) (bool, error) {
	fi, err := db.GetFileInfo(path)
	if err != nil && err != sql.ErrNoRows {
		return false, err
	}

	if err == sql.ErrNoRows {
		return true, nil
	}
	if !info.ModTime().Truncate(time.Millisecond).After(fi.ModTime.Truncate(time.Millisecond)) {
		return false, nil
	}

	hash, err := getFileHash(path)
	if err != nil {
		return false, err
	}
	if hash == fi.Hash {
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

func getFilesToBackup(db *DB, root string, maxDepth int, sizeThreshold int) ([]*BackupRequest, error) {
	dirQueue := []QueueItem{{Path: root, Depth: 0}}
	backupRequests := []*BackupRequest{}

	// Scan files and compare mod times
	for len(dirQueue) > 0 {
		dir := dirQueue[0]
		dirQueue = dirQueue[1:]

		req := &BackupRequest{
			Path:  dir.Path,
			IsDir: true,
		}

		log.Printf("scanning dir %q depth %d", dir.Path, dir.Depth)

		if maxDepth > 0 && dir.Depth > maxDepth {
			// Check if directory needs backup.
			files, err := getFilesToBackup(db, dir.Path, -1, sizeThreshold)
			if err != nil {
				return nil, err
			}
			if len(files) > 0 {
				// This is a terminal directory (i.e. we just back up the whole thing as an archive)
				req.Recurse = true
				for _, f := range files {
					req.DirtySubFiles = append(req.DirtySubFiles, f.Path)
				}
				backupRequests = append(backupRequests, req)
			}
			continue
		}

		files, err := os.ReadDir(dir.Path)
		if err != nil {
			log.Fatalf("error scanning directory: %v", err)
		}

		for _, file := range files {
			path := filepath.Join(dir.Path, file.Name())

			if file.IsDir() {
				dirQueue = append(dirQueue, QueueItem{
					Path:  path,
					Depth: dir.Depth + 1,
				})
				continue
			}

			info, err := file.Info()
			if err != nil {
				log.Fatal(err)
			}

			if doBackupFile(path) {
				dirty, err := doesFileNeedBackup(db, path, info)
				if err != nil {
					log.Fatal(err)
				}

				log.Printf("%s (%d bytes) - %v", path, info.Size(), info.ModTime())
				if info.Size() > int64(sizeThreshold) {
					if dirty {
						// If the file meets the given criteria and is dirty, back it up individually
						backupRequests = append(backupRequests, &BackupRequest{
							Path:  path,
							IsDir: false,
						})
					} else {
						log.Printf("%s already backed up, skipping", path)
					}
				} else {
					// Make sure to include _all_ files below the individual backup threshold - if this directory
					// contains _any_ dirty files below the size threshold, we want to make sure we include _all_
					// of them in the archive.
					req.SubFilesToBackup = append(req.SubFilesToBackup, path)
					if dirty {
						req.DirtySubFiles = append(req.DirtySubFiles, path)
					}
				}
			} else {
				log.Printf("skipping file %s", path)
			}
		}

		if len(req.DirtySubFiles) > 0 {
			backupRequests = append(backupRequests, req)
		} else {
			log.Printf("directory %q has no dirty files, skipping", req.Path)
		}
	}
	return backupRequests, nil
}
