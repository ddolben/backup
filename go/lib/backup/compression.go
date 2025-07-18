package backup

import (
	"archive/tar"
	"compress/gzip"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func decompressFile(sourcePath string, destinationDir string) (string, error) {
	archiveFile, err := os.Open(sourcePath)
	if err != nil {
		return "", err
	}
	defer archiveFile.Close()

	gzr, err := gzip.NewReader(archiveFile)
	if err != nil {
		return "", err
	}
	defer gzr.Close()

	destinationFilename := filepath.Join(destinationDir, filepath.Base(sourcePath))
	destinationFilename = strings.TrimSuffix(destinationFilename, ".gz")
	destinationFile, err := os.Create(destinationFilename)
	if err != nil {
		return "", err
	}
	defer destinationFile.Close()

	_, err = io.Copy(destinationFile, gzr)
	if err != nil {
		return "", err
	}

	return destinationFilename, nil
}

// Mostly from https://medium.com/@skdomino/taring-untaring-files-in-go-6b07cf56bc07
func unTar(path string, destinationDir string) error {
	archiveFile, err := os.Open(path)
	if err != nil {
		return err
	}
	defer archiveFile.Close()

	gzr, err := gzip.NewReader(archiveFile)
	if err != nil {
		return err
	}
	defer gzr.Close()

	tr := tar.NewReader(gzr)

	for {
		header, err := tr.Next()

		switch {
		// if no more files are found return
		case err == io.EOF:
			return nil

		// return any other error
		case err != nil:
			return err

		// if the header is nil, just skip it (not sure how this happens)
		case header == nil:
			continue
		}

		// the target location where the dir/file should be created
		target := filepath.Join(destinationDir, header.Name)
		log.Printf("extracting %q", target)

		// the following switch could also be done using fi.Mode(), not sure if there
		// a benefit of using one vs. the other.
		// fi := header.FileInfo()

		// check the file type
		switch header.Typeflag {

		// if its a dir and it doesn't exist create it
		case tar.TypeDir:
			if _, err := os.Stat(target); err != nil {
				if err := os.MkdirAll(target, 0755); err != nil {
					return err
				}
			}

		// if it's a file create it
		case tar.TypeReg:
			// Create all intermediate directories required
			dirPath := filepath.Dir(target)
			if _, err := os.Stat(dirPath); err != nil {
				log.Printf("creating intermediate directories: %q", dirPath)
				if err := os.MkdirAll(dirPath, 0755); err != nil {
					return err
				}
			}
			f, err := os.OpenFile(target, os.O_CREATE|os.O_RDWR, os.FileMode(header.Mode))
			if err != nil {
				return err
			}

			// copy over contents
			if _, err := io.Copy(f, tr); err != nil {
				return err
			}

			// Set the modtime to match the tar archive's header.
			err = os.Chtimes(target, time.Now(), header.ModTime)
			if err != nil {
				return err
			}

			// manually close here after each file operation; defering would cause each file close
			// to wait until all operations have completed.
			f.Close()
		}
	}
}
