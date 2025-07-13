# Personal Backup

A set of tools for personal file backups to S3.

## Local setup

### minio to simulate S3

### Testing

The tests require minio - make sure to run

```
docker compose up -d
```

before running tests. The script to run the dev docker will attach to the same network.

## To Do

- Clean up logging
- Upload the DB to storage, and check before upload/recovery whether the DB has changed since last upload
- Config file vs. DB args (spf13/viper?)
