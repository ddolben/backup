# Personal Backup

A set of tools for personal file backups to S3.

## Local setup

```
brew install rclone
```

### minio to simulate S3

### Testing

The tests require minio - make sure to run

```
docker compose up -d
```

before running tests. The script to run the dev docker will attach to the same network.

