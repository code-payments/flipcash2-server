.PHONY: all
all: test

.PHONY: test
test:
	@go test -cover -count=1 ./...

.PHONY: test-integration
test-integration:
	@go test -tags integration -cover -count=1 -timeout=5m ./...
