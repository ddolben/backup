package backup

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"local/backup/s3_helpers"
	"log"
	"os"
	"path/filepath"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func backupFile(client *s3.Client, bucket string, prefix string, localRoot string, localPath string) error {
	key := localPath
	key = filepath.Join(prefix, key)
	absolutePath := filepath.Join(localRoot, localPath)

	log.Printf("backing up file %q to %q", localPath, key)
	return s3_helpers.UploadFile(client, bucket, key, absolutePath)
}

// Mostly from https://www.arthurkoziel.com/writing-tar-gz-files-in-go/
func backupDirectory(
	client *s3.Client,
	bucket string,
	prefix string,
	localRoot string,
	// This should be relative to the root
	localPath string,
	files []string,
) error {
	key := filepath.Join(prefix, localPath, "_files.tar.gz")
	log.Printf("backing up directory %q -> %q", localPath, key)

	// Create a buffer to write the files into
	buf := &bytes.Buffer{}

	// Streams for tar archive and gzip
	gw := gzip.NewWriter(buf)
	tw := tar.NewWriter(gw)

	// Scan all the specified files and back them up to the archive.
	for _, filename := range files {
		log.Printf("  archiving file %q", filename)
		absoluteArchiveRoot := filepath.Join(localRoot, localPath)
		absoluteFilename := filepath.Join(localRoot, filename)
		if err := addFileToArchive(tw, absoluteArchiveRoot, absoluteFilename); err != nil {
			return fmt.Errorf("failed to add file %q to archive: %+v", filename, err)
		}
	}

	// Explicitly close those writers so the tar archive and gzip file are complete before we write
	// the buffer to S3. Make sure to close the tar writer first to flush all archive bytes to the
	// gzip compressor.
	// TODO: ideally we'd do this in a streaming manner
	if err := tw.Close(); err != nil {
		log.Fatal(err)
	}
	if err := gw.Close(); err != nil {
		log.Fatal(err)
	}

	// Write the results of the buffer to s3
	_, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: &bucket,
		Key:    &key,
		// TODO: is this optimal?
		Body: bytes.NewReader(buf.Bytes()),
	})
	if err != nil {
		return fmt.Errorf("failed to upload local directory %q to %q: %v", localPath, key, err)
	}
	return nil
}

func addFileToArchive(tw *tar.Writer, baseDir string, filename string) error {
	// Open the file which will be written into the archive
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	// Get FileInfo about our file providing file size, mode, etc.
	info, err := file.Stat()
	if err != nil {
		return err
	}

	// Create a tar Header from the FileInfo data
	header, err := tar.FileInfoHeader(info, info.Name())
	if err != nil {
		return err
	}

	// Use full path as name (FileInfoHeader only takes the basename)
	// If we don't do this the directory strucuture would
	// not be preserved
	// https://golang.org/src/archive/tar/common.go?#L626
	relativePath, err := filepath.Rel(baseDir, filename)
	if err != nil {
		return err
	}
	header.Name = relativePath

	// Write file header to the tar archive
	err = tw.WriteHeader(header)
	if err != nil {
		return err
	}

	// Copy file content to tar archive
	_, err = io.Copy(tw, file)
	if err != nil {
		return err
	}

	return nil
}
