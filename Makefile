VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
GHCR_IMAGE ?= ghcr.io/gftdcojp/nats-jetstream-kv-resp
LDFLAGS := -ldflags "-X main.version=$(VERSION)"

.PHONY: all build test lint clean

all: build

build:
	go build $(LDFLAGS) -o bin/nats-jetstream-kv-resp ./cmd/nats-jetstream-kv-resp

test:
	go test -race -count=1 ./...

lint:
	golangci-lint run ./...

clean:
	rm -rf bin/

.PHONY: docker
docker:
	docker build --build-arg VERSION=$(VERSION) -t nats-jetstream-kv-resp:$(VERSION) -f deploy/docker/Dockerfile .

.PHONY: docker-ghcr
docker-ghcr:
	docker buildx build --platform linux/amd64,linux/arm64 \
		--build-arg VERSION=$(VERSION) \
		-t $(GHCR_IMAGE):$(VERSION) \
		-t $(GHCR_IMAGE):latest \
		-f deploy/docker/Dockerfile --push .

.PHONY: dev
dev:
	docker compose -f deploy/docker/docker-compose.yaml up --build
