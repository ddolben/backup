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
	Depth int32
}

type BackupRequest struct {
	Path          string
	IsDir         bool
	DirtySubFiles []string
}

func getFilesToBackup(db *DB, root string, maxDepth int32, earlyExit bool) ([]*BackupRequest, error) {
	dirQueue := []QueueItem{{Path: root, Depth: 0}}
	backupRequests := []*BackupRequest{}

	// Scan files and compare mod times
	for len(dirQueue) > 0 {
		dir := dirQueue[0]
		dirQueue = dirQueue[1:]

		if maxDepth > 0 && dir.Depth > maxDepth {
			// Check if directory needs backup.
			files, err := getFilesToBackup(db, dir.Path, -1, false)
			if err != nil {
				return nil, err
			}
			if len(files) > 0 {
				req := &BackupRequest{
					Path:  dir.Path,
					IsDir: true,
				}
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
				backupRequests = append(backupRequests, &BackupRequest{
					Path:  path,
					IsDir: false, // not strictly necessary since false is the bool default init value
				})
				log.Printf("%s - %v", path, info.ModTime())
				if earlyExit {
					return backupRequests, nil
				}
			} else {
				log.Printf("%s already backed up, skipping", path)
			}
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
	flag.Parse()

	// Number of directories deep to scan.
	const maxDepth = 1

	// Load the db
	db, err := newDB(*fMetaDb)
	if err != nil {
		log.Fatalf("error loading db: %v", err)
	}

	log.Println("> Scanning files")
	paths, err := getFilesToBackup(db, *fRootDir, maxDepth, false)
	if err != nil {
		log.Fatalf("error finding files to backup: %v", err)
	}
	log.Println("< Scanning files")

	log.Println("> Backing up files")
	for _, path := range paths {
		if path.IsDir {
			log.Printf("Backing up directory: %s", path.Path)
			for _, f := range path.DirtySubFiles {
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
