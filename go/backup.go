package main

import (
	"fmt"
	"local/backup/s3_helpers"
	"log"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const rcloneName = "local-minio"
const bucket = "test-data"

func backupFile(client *s3.Client, bucket string, localPath string) error {
	dirname := filepath.Dir(localPath)
	if dirname == "." {
		dirname = ""
	} else {
		dirname += "/"
	}

	key := filepath.Join(dirname, filepath.Base(localPath))

	log.Printf("backing up file %q to %q", localPath, key)
	return s3_helpers.UploadFile(client, bucket, key, localPath)
}

func backupDirectory(path string) error {
	fmt.Printf("dir %q -> %q\n", path, path)
	return nil

	cmd := exec.Command(
		"rclone", "copy" /*"${args[@]}",*/, path, rcloneName+":"+bucket+"/"+path)

	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	// Run still runs the command and waits for completion
	// but the output is instantly piped to Stdout
	return cmd.Run()
}
