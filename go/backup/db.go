package backup

import (
	"database/sql"
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
