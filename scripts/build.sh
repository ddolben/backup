#!/usr/bin/env bash

script_dir="$(cd "$(dirname "$0")" && pwd)"

export GOOS=darwin
export GOARCH=arm64

cd "${script_dir}/../go"
go build -o ../go-bin/backup .
go build -o ../go-bin/clear-bucket ./experimental/s3-clear-prefix.go

