.PHONY: build test race lint bench clean
BIN := bin

build:
	go build -o $(BIN)/ ./cmd/...

test:
	go test -count=1 ./...

race:
	go test -race -count=3 ./...

lint:
	gofmt -l . | (! grep .)
	go vet ./...
	golangci-lint run ./...

# 2000 trivial jobs through a real coordinator, for a few worker counts and fsync policies
bench: build
	for w in 2 8 32; do for s in batch always; do $(BIN)/loadgen -n 2000 -workers $$w -sync $$s | tail -3; done; done

clean:
	rm -rf $(BIN)
