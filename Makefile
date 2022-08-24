#

# Makefile for the downloader service.
#
# SYNOPSIS:
#
#   make             - format and compile the entire program along with its dependencies
#   make fmt         - run gofmt to report any code formatting errors
#   make build       - compile the program but not install the results
#   make check       - run the package's test suite
#   make install     - compile the program and create the executables
#   make clean       - remove all files generated by building the program
#   make distclean   - remove all files generated by make
#   make lint        - run golint linter on source code
#   make vet         - run the Go vet command on source code

#

.PHONY: install fmt build check install clean distclean lint vet docker-image-build docker-build docker-clean

all: fmt build

fmt:
	@if [ -n "$(shell gofmt -l . | grep -v vendor/)" ]; then \
		echo "Source code needs re-formatting! Use 'go fmt' manually."; \
		false; \
	fi

build:
	go build

docker-clean:
	rm -rf docker-build

docker-image-build:
	docker image build -f Dockerfile.build -t downloader-build:latest .

docker-build: docker-clean docker-image-build
	docker container run --rm -v $(CURDIR):/downloader -v /tmp:/build downloader-build:latest
	mkdir -p docker-build
	cp /tmp/downloader docker-build/

check: build
	go test -race -p 1 ./...

install: all
	go install -v

clean:
	rm -rf vendor/
	go clean

distclean: clean
	go clean -i -cache -testcache

lint:
	golint ./...

vet:
	go vet ./...
