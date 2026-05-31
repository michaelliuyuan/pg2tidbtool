FROM node:20-alpine AS frontend

WORKDIR /app/web/frontend
COPY web/frontend/package.json web/frontend/package-lock.json* ./
RUN npm ci
COPY web/frontend/ .
RUN npm run build

FROM golang:1.22-alpine AS builder

RUN apk add --no-cache git make curl

# Download tidb-lightning for embedding
ARG TIDB_LIGHTNING_VERSION=v7.1.9
RUN curl -fsSL https://download.pingcap.org/tidb-toolkit-${TIDB_LIGHTNING_VERSION}-linux-amd64.tar.gz -o /tmp/toolkit.tar.gz \
    && tar -xzf /tmp/toolkit.tar.gz -C /tmp \
    && cp /tmp/tidb-toolkit-${TIDB_LIGHTNING_VERSION}-linux-amd64/bin/tidb-lightning /tmp/tidb-lightning

WORKDIR /app
COPY go.mod go.sum* ./
RUN go mod download || true
COPY . .
COPY --from=frontend /app/web/dist cmd/static/

# Embed tidb-lightning into the binary
RUN cp /tmp/tidb-lightning internal/lightning/tidb-lightning \
    && go mod tidy \
    && CGO_ENABLED=0 go build -ldflags="-s -w" -o /pg2tidb . \
    && echo "placeholder" > internal/lightning/tidb-lightning

FROM alpine:3.19
RUN apk add --no-cache ca-certificates tzdata
COPY --from=builder /pg2tidb /usr/local/bin/pg2tidb
COPY configs/config.yaml /etc/pg2tidb/config.yaml

EXPOSE 8080

ENTRYPOINT ["pg2tidb"]
CMD ["web", "--port", "8080"]
