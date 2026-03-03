FROM golang:1.25-alpine AS builder

ARG VERSION=dev

WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w -X main.version=${VERSION}" -o /nats-jetstream-kv-resp ./cmd/nats-jetstream-kv-resp

FROM alpine:3.20

RUN apk --no-cache add ca-certificates tzdata
RUN adduser -D -u 1000 kvresp

COPY --from=builder /nats-jetstream-kv-resp /usr/local/bin/nats-jetstream-kv-resp

USER kvresp
EXPOSE 6380 6381 9090
ENTRYPOINT ["nats-jetstream-kv-resp"]
CMD ["-config", "/etc/kvresp/config.yaml"]
