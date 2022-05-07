# TODO: a golang docker build exmaple is provided by default, remove this if it is not applicable to your service
# Build the manager binary
FROM golang:1.18 as builder

WORKDIR /workspace

# Copy the go source
COPY . .

# Build
RUN CGO_ENABLED=0 go build

FROM alpine:3.10
WORKDIR /

COPY --from=builder /workspace/tidb-gateway .

USER 65532:65532

ENTRYPOINT ["/tidb-gateway"]