all: test

test: fmt vet
	go test -v

fmt:
	go fmt ./...

vet:
	go vet ./...
