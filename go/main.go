package main

import (
	"crypto/md5"
	"database/sql"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"time"
)

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
	Path          string
	IsDir         bool
	Recurse       bool
	DirtySubFiles []string
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

			dirty, err := doesFileNeedBackup(db, path, info)
			if err != nil {
				log.Fatal(err)
			}

			if dirty {
				if info.Size() > int64(sizeThreshold) {
					// If the file meets the given criteria, back it up individually
					backupRequests = append(backupRequests, &BackupRequest{
						Path:  path,
						IsDir: false,
					})
					continue
				} else {
					// Otherwise add it to this directory's dirty files list
					req.DirtySubFiles = append(req.DirtySubFiles, path)
				}
				log.Printf("%s (%d bytes) - %v", path, info.Size(), info.ModTime())
			} else {
				log.Printf("%s already backed up, skipping", path)
			}
		}

		if len(req.DirtySubFiles) > 0 {
			backupRequests = append(backupRequests, req)
		}
	}
	return backupRequests, nil
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

func main() {
	fMetaDb := flag.String("db", "backup.db", "database location for local cache storage")
	fRootDir := flag.String("dir", ".", "root directory for backup operation")
	fMaxDepth := flag.Int("max_depth", -1, "max subfolder depth to recurse into before just archiving the whole tree (-1 means no max)")
	fIndividualSizeThreshold := flag.Int("size_threshold", 2048, "size threshold, in bytes, above which files get backed up individually")
	flag.Parse()

	// Load the db
	db, err := newDB(*fMetaDb)
	if err != nil {
		log.Fatalf("error loading db: %v", err)
	}

	log.Println("> Scanning files")
	paths, err := getFilesToBackup(db, *fRootDir, *fMaxDepth, *fIndividualSizeThreshold)
	if err != nil {
		log.Fatalf("error finding files to backup: %v", err)
	}
	log.Println("< Scanning files")

	log.Println("> Backing up files")
	for _, path := range paths {
		if path.IsDir {
			log.Printf("Backing up directory: %s, dirty files:", path.Path)
			for _, f := range path.DirtySubFiles {
				log.Printf("  %s", f)
				markFile(db, f)
			}
			continue
		}

		log.Printf("Backing up file: %s", path.Path)
		err := markFile(db, path.Path)
		if err != nil {
			log.Fatalf("error marking file as processed: %v", err)
		}
	}
	log.Println("< Backing up files")
}
