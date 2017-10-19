.PHONY: install build test lint vet fmt clean list

install: vet fmt test
	go build -v
	go install -v

test:
	go test ./...

lint:
	golint ./...

vet:
	go vet ./...

fmt:
	! gofmt -d -e -s *.go **/*.go 2>&1 | tee /dev/tty | read

clean:
	go clean

list:
	@$(MAKE) -pRrq -f $(lastword $(MAKEFILE_LIST)) : 2>/dev/null | awk -v RS= -F: '/^# File/,/^# Finished Make data base/ {if ($$1 !~ "^[#.]") {print $$1}}' | sort | egrep -v -e '^[^[:alnum:]]' -e '^$@$$' | xargs
