#!/usr/bin/env bash
set -euo pipefail

# 依赖：
#   protoc
#   go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
#   go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

mkdir -p internal/rpc/pb

protoc \
  --proto_path=proto \
  --go_out=internal/rpc/pb --go_opt=paths=source_relative \
  --go-grpc_out=internal/rpc/pb --go-grpc_opt=paths=source_relative \
  proto/storagenode.proto

echo "generated internal/rpc/pb/*.pb.go"
