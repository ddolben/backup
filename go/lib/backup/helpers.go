package backup

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"local/backup/lib/logging"
	"log"
	"os"
	"path/filepath"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// XXX: unused right now, since we need the tar archive to preserve modtimes
func backupFileNoArchive(logger logging.Logger, client *s3.Client, bucket string, prefix string, localRoot string, localPath string) error {
	key := localPath + ".gz"
	key = filepath.Join(prefix, key)
	absolutePath := filepath.Join(localRoot, localPath)

	logger.Verbosef("backing up file %q to %q", localPath, key)

	// Create a buffer to write the file into
	buf := &bytes.Buffer{}

	// Streams for tar archive and gzip
	gw := gzip.NewWriter(buf)

	// Write the file to the gzip writer
	file, err := os.Open(absolutePath)
	if err != nil {
		return fmt.Errorf("failed to open file %q: %+v", localPath, err)
	}
	defer file.Close()
	_, err = io.Copy(gw, file)
	if err != nil {
		return fmt.Errorf("failed to copy file %q to gzip writer: %+v", localPath, err)
	}

	// Close writers to complete the archive
	if err := gw.Close(); err != nil {
		return fmt.Errorf("failed to close gzip writer: %v", err)
	}

	// Write the results of the buffer to s3
	_, err = client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: &bucket,
		Key:    &key,
		Body:   bytes.NewReader(buf.Bytes()),
	})
	if err != nil {
		return fmt.Errorf("failed to upload file %q to %q: %v", localPath, key, err)
	}
	return nil
}

func backupFile(
	logger logging.Logger,
	client *s3.Client,
	bucket string,
	prefix string,
	localRoot string,
	// Relative to the local root
	filePath string,
) error {
	archiveName := filePath + ".tar.gz"

	logger.Verbosef(
		"backing up file %q to %q",
		filePath,
		archiveName,
	)

	// XXX: this isn't optimal, since it's relatively inefficient to wrap a single file with a tar
	// archive. For now we need it to preserve modtimes.
	return backupFilesToArchive(
		logger,
		client,
		bucket,
		prefix,
		archiveName,
		localRoot,
		filepath.Dir(filePath),
		[]string{filePath},
	)
}

// Mostly from https://www.arthurkoziel.com/writing-tar-gz-files-in-go/
func backupDirectory(
	logger logging.Logger,
	client *s3.Client,
	bucket string,
	prefix string,
	localRoot string,
	// This should be relative to the root
	localBatchRoot string,
	files []string,
) error {
	return backupFilesToArchive(
		logger,
		client,
		bucket,
		prefix,
		filepath.Join(localBatchRoot, "_files.tar.gz"),
		localRoot,
		localBatchRoot,
		files,
	)
}

func backupFilesToArchive(
	logger logging.Logger,
	client *s3.Client,
	bucket string,
	prefix string,
	// S3 key relative to the prefix
	archiveName string,
	// Root of the local backup directory
	localRoot string,
	// Relative to the local root
	localBatchRoot string,
	files []string,
) error {
	key := filepath.Join(prefix, archiveName)
	logger.Verbosef("backing up directory %q -> %q", localBatchRoot, key)

	// Create a buffer to write the files into
	buf := &bytes.Buffer{}

	// Streams for tar archive and gzip
	gw := gzip.NewWriter(buf)
	tw := tar.NewWriter(gw)

	// Scan all the specified files and back them up to the archive.
	for _, filename := range files {
		logger.Verbosef("  archiving file %q", filename)
		absoluteArchiveRoot := filepath.Join(localRoot, localBatchRoot)
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
		return fmt.Errorf("failed to upload local directory %q to %q: %v", localBatchRoot, key, err)
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

	// Update the header's format to preserve sub-second modtime resolution (see https://pkg.go.dev/archive/tar#Format)
	header.Format = tar.FormatPAX

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
