.PHONY: build test lint clean fmt vet

BINARY_NAME := pg2tidb
VERSION := $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
LDFLAGS := -s -w -X main.Version=$(VERSION)
BUILD_DIR := build

build:
	@echo "Building $(BINARY_NAME)..."
	@mkdir -p $(BUILD_DIR)
	go build -ldflags "$(LDFLAGS)" -o $(BUILD_DIR)/$(BINARY_NAME) .

test:
	go test -v -race -count=1 ./...

test-cover:
	go test -v -race -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html

lint:
	golangci-lint run ./...

vet:
	go vet ./...

fmt:
	gofmt -w .
	goimports -w .

clean:
	rm -rf $(BUILD_DIR) coverage.out coverage.html

web-frontend:
	@echo "Building frontend..."
	cd web/frontend && npm ci && npm run build
	rm -rf cmd/static/assets cmd/static/favicon.svg cmd/static/icons.svg
	cp -r web/dist/* cmd/static/

build-web: web-frontend
	@echo "Building $(BINARY_NAME) with embedded web UI..."
	@mkdir -p $(BUILD_DIR)
	CGO_ENABLED=0 go build -ldflags "$(LDFLAGS)" -o $(BUILD_DIR)/$(BINARY_NAME) .

all: fmt vet test build
