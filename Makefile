PACKAGES := $$(go list ./...| grep -vE 'vendor' | grep -vE 'nuclear')
TEST_DIRS := $(shell find . -iname "*_test.go" -exec dirname {} \; | uniq | grep -vE 'vendor')

default: build

test:
	go test -race $(TEST_DIRS)

build:
	go build -o bin/gravity-operator cmd/operator/main.go
	go build -o bin/gravity-gatekeeper cmd/gatekeeper/main.go

build-linux:
	GOARCH=amd64 GOOS=linux go build -o bin/gravity-operator-amd64 cmd/operator/main.go
	GOARCH=amd64 GOOS=linux go build -o bin/gravity-gatekeeper-amd64 cmd/gatekeeper/main.go

image: build-linux
	docker build -t moiot/gravity-operator -f Dockerfile.operator .
	docker build -t moiot/gravity-gatekeeper -f Dockerfile.gatekeeper .