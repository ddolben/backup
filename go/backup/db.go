package backup

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/glebarez/go-sqlite"
)

type FileInfo struct {
	ModTime time.Time
	Hash    string
	Batch   string
}

type DB struct {
	db *sql.DB
}

func newDB(path string) (*DB, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	err = initDB(db)
	if err != nil {
		return nil, err
	}
	return &DB{
		db: db,
	}, nil
}

func initDB(db *sql.DB) error {
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS files (
			path text,
			mod_time timestamptz,
			hash text,
			-- The batch that this file belongs to
			batch text,
			PRIMARY KEY (path)
		)
	`)
	return err
}

func (db *DB) DumpDB() error {
	rows, err := db.db.Query(`
		SELECT
			path,
			batch,
			mod_time,
			hash
		FROM files
	`)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var path string
		var batch string
		var ts string
		var hash string
		if err := rows.Scan(&path, &batch, &ts, &hash); err != nil {
			return err
		}
		modTime, err := time.Parse("2006-01-02 15:04:05.000", ts)
		if err != nil {
			return err
		}
		log.Printf("  %v %v %v %v", path, batch, modTime, hash)
	}
	return nil
}

func (db *DB) GetFileInfo(path string) (*FileInfo, error) {
	row := db.db.QueryRow("SELECT mod_time, hash, batch FROM files WHERE path = ?", path)
	if row.Err() != nil {
		return nil, row.Err()
	}
	fileInfo := &FileInfo{}
	var ts string
	err := row.Scan(&ts, &fileInfo.Hash, &fileInfo.Batch)
	if err != nil {
		return nil, err
	}
	fileInfo.ModTime, err = time.Parse("2006-01-02 15:04:05.000", ts)
	if err != nil {
		return nil, err
	}
	return fileInfo, nil
}

func (db *DB) MarkFile(path string, modTime time.Time, hash string, batch string) error {
	_, err := db.db.Exec(`
		INSERT INTO files (
			path, mod_time, hash, batch
		)
		VALUES ( ?, ?, ?, ? )
		ON CONFLICT (path)
		DO UPDATE SET
			mod_time = excluded.mod_time,
			hash = excluded.hash,
			batch = excluded.batch
	`, path, modTime.UTC().Format("2006-01-02 15:04:05.000"), hash, batch)
	return err
}

type BatchMeta struct {
	Path         string
	IsSingleFile bool
}

func (db *DB) GetExistingBatches() ([]BatchMeta, error) {
	// TODO: what happens if the DB has files in a batch with a name that is also the same as one of the filenames?
	rows, err := db.db.Query(`
		SELECT
			batch,
			count(*) as num_files,
			sum(is_dir) as num_grouped_files
		FROM (
			SELECT
				batch,
				path,
				CASE
					WHEN batch != path THEN 1
					ELSE 0
				END as is_dir
			FROM files
		)
		GROUP BY batch
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var batches []BatchMeta
	for rows.Next() {
		var batch string
		var numFiles int64
		var numGroupedFiles int64
		if err := rows.Scan(&batch, &numFiles, &numGroupedFiles); err != nil {
			return nil, err
		}
		if numGroupedFiles > 0 && numGroupedFiles != numFiles {
			return nil, fmt.Errorf("detected a batch with multiple files, where one of the filenames matches the batch name: %q", batch)
		}
		batches = append(batches, BatchMeta{
			Path:         batch,
			IsSingleFile: numGroupedFiles == 0,
		})
	}
	return batches, nil
}
