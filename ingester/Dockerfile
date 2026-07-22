# syntax=docker/dockerfile:1

# ---- build stage ------------------------------------------------------------
# Pinned toolchain. go.mod requires go 1.25 (pulled in transitively by the
# testcontainers integration deps); keep this in sync with the `go` directive.
FROM golang:1.25-bookworm AS build

WORKDIR /src

# Cache modules first.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

# Build the static binary. CGO disabled -> fully static, runs on scratch/distroless.
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux GOFLAGS=-trimpath \
    go build -ldflags="-s -w" -o /out/ingester ./cmd/ingester

# ---- runtime stage ----------------------------------------------------------
# distroless static, nonroot variant: no shell, no package manager, runs as
# uid/gid 65532. Pinned by digest-friendly tag.
FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /out/ingester /usr/local/bin/ingester

# Metrics/health port (see INGESTER_METRICS_ADDR).
EXPOSE 9100

USER 65532:65532
ENTRYPOINT ["/usr/local/bin/ingester"]
