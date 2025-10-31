# syntax=docker/dockerfile:1
FROM golang:1.22-alpine AS builder
WORKDIR /src
ENV CGO_ENABLED=0
ENV GOTOOLCHAIN=auto
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o /knxctl ./cmd/knxctl

FROM alpine:3.19
RUN apk add --no-cache ca-certificates
COPY --from=builder /knxctl /usr/local/bin/knxctl
ENTRYPOINT ["knxctl"]
