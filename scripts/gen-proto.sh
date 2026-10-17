#!/usr/bin/env bash
# Generates the Go and Python code for proto/ with a single protoc: the one bundled in
# grpcio-tools. The Go plugins are pinned by the tool directives in go.mod.
set -euo pipefail
cd "$(dirname "$0")/.."

tools="$PWD/bin/tools"
mkdir -p "$tools"
go build -o "$tools/" google.golang.org/protobuf/cmd/protoc-gen-go google.golang.org/grpc/cmd/protoc-gen-go-grpc

uv run --group dev python -m grpc_tools.protoc -Iproto \
  --plugin=protoc-gen-go="$tools/protoc-gen-go" \
  --plugin=protoc-gen-go-grpc="$tools/protoc-gen-go-grpc" \
  --go_out=gen/go --go_opt=paths=source_relative \
  --go-grpc_out=gen/go --go-grpc_opt=paths=source_relative \
  --python_out=python --pyi_out=python --grpc_python_out=python \
  proto/agentos/v1/gateway.proto proto/agentos/v1/control.proto
