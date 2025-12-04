FROM golang:1.25.4-bookworm AS builder

WORKDIR /app

ENV GOPATH=/app/.go
ENV PATH="${PATH}:${GOPATH}/bin"    

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN go build -o /app/yandex-kafka -ldflags "-s -w" ./cmd/producer && \
    go build -o /app/yandex-kafka-consumer -ldflags "-s -w" ./cmd/consumer

FROM debian:12-slim AS runtime

WORKDIR /app

COPY --from=builder /app/yandex-kafka /app/yandex-kafka
COPY --from=builder /app/yandex-kafka-consumer /app/yandex-kafka-consumer
RUN chmod +x /app/yandex-kafka

USER 65535
EXPOSE 8000
