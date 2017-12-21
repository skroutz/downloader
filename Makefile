.PHONY: install build test lint vet fmt clean

install: fmt test
	go install -v

test:
	go test -race
	go test -race ./notifier/... ./processor/... ./api/... ./stats/... ./storage/...

lint:
	golint ./...

vet:
	go vet ./...

fmt:
	! gofmt -d -e -s *.go **/*.go 2>&1 | tee /dev/tty | read

clean:
	go clean
