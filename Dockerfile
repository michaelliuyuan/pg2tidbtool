FROM golang:1.22-alpine AS builder

RUN apk add --no-cache git make

WORKDIR /app
COPY go.mod go.sum* ./
RUN go mod download || true
COPY . .
RUN go mod tidy && CGO_ENABLED=0 go build -ldflags="-s -w" -o /pg2tidb .

FROM alpine:3.19
RUN apk add --no-cache ca-certificates tzdata
COPY --from=builder /pg2tidb /usr/local/bin/pg2tidb
COPY configs/config.yaml /etc/pg2tidb/config.yaml

ENTRYPOINT ["pg2tidb"]
CMD ["--help"]
