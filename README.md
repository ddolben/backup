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

- Clean up logging in the recovery pathway
- Recovery seems not to truncate files (leaves garbage at the end of a file if the file gets shorter)
- Should recovery delete files _not_ present in the backup?
- Config file vs. DB args (spf13/viper?)

