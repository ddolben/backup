ROOT_DIR := $(dir $(realpath $(lastword $(MAKEFILE_LIST))))
SRC_DIR = $(ROOT_DIR)/go
DIST_DIR = $(ROOT_DIR)/dist
BIN_DIR = $(DIST_DIR)/bin

dbackup:
	mkdir -p $(DIST_DIR)
	cd $(SRC_DIR) && go build -o $(BIN_DIR)/dbackup ./cmd/dbackup/

build: dbackup

test:
	cd $(SRC_DIR) && go test ./...
