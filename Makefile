.PHONY: all
all: test

.PHONY: test
test:
	@go test -cover -count=1 ./...

.PHONY: test-integration
test-integration:
	@go get github.com/steebchen/prisma-client-go@v0.47.0
	@go test -tags integration -cover -count=1 -timeout=5m ./...
