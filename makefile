ROOT_DIR := $(dir $(realpath $(lastword $(MAKEFILE_LIST))))
SRC_DIR = $(ROOT_DIR)/go
DIST_DIR = $(ROOT_DIR)/dist
BIN_DIR = $(DIST_DIR)
BINARY = $(BIN_DIR)/dbackup
SRC_FILES = $(shell find $(SRC_DIR) -type f -name '*.go')

$(BINARY): $(SRC_FILES)
	mkdir -p $(DIST_DIR)
	cd $(SRC_DIR) && go build -o $(BINARY) ./cmd/dbackup/

.PHONY: build
build: $(BINARY)

.PHONY: clean
clean:
	rm -r $(DIST_DIR)/*

.PHONY: test
test:
	cd $(SRC_DIR) && go test ./...
