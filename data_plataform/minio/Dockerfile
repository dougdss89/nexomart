# MinIO stopped publishing images to Docker Hub/quay (minio/minio and minio/mc no longer exist), so
# both binaries are built from the pinned release tags. One image carries `minio` (server) and `mc`
# (healthcheck + the minio-init bucket job).
FROM golang:1.24-alpine AS build
RUN apk add --no-cache git bash
ARG MINIO_TAG=RELEASE.2025-10-15T17-29-55Z
ARG MC_TAG=RELEASE.2025-08-13T08-35-41Z
ENV CGO_ENABLED=0 GOTOOLCHAIN=auto

RUN git clone --depth 1 --branch ${MINIO_TAG} https://github.com/minio/minio /src/minio && \
    cd /src/minio && \
    go build -trimpath -ldflags "$(go run buildscripts/gen-ldflags.go)" -o /out/minio .

RUN git clone --depth 1 --branch ${MC_TAG} https://github.com/minio/mc /src/mc && \
    cd /src/mc && \
    go build -trimpath -ldflags "-s -w" -o /out/mc .

FROM alpine:3.21
RUN apk add --no-cache ca-certificates curl
COPY --from=build /out/minio /out/mc /usr/bin/
EXPOSE 9000 9001
VOLUME ["/data"]
ENTRYPOINT ["/usr/bin/minio"]
