#!/usr/bin/env bash
set -euo pipefail

# Dependencies:
#   protoc
#   go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
#   go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

need_cmd() {
  local c="$1"
  if ! command -v "$c" >/dev/null 2>&1; then
    echo "ERROR: '$c' not found in PATH" >&2
    exit 1
  fi
}

need_cmd protoc
need_cmd protoc-gen-go
need_cmd protoc-gen-go-grpc

# Output dirs (match each proto's go_package)
mkdir -p internal/rpc/pb
mkdir -p internal/rpc/pbmeta
mkdir -p internal/rpc/pbcoord

gen_one() {
  local proto_file="$1"
  local out_dir="$2"

  echo "[protoc] generating ${proto_file} -> ${out_dir}"
  protoc \
    --proto_path=proto \
    --go_out="${out_dir}" --go_opt=paths=source_relative \
    --go-grpc_out="${out_dir}" --go-grpc_opt=paths=source_relative \
    "${proto_file}"
}

# storagenode API
gen_one proto/storagenode.proto internal/rpc/pb

# metacoordinator (ShardView / epoch CAS)
gen_one proto/metacoordinator.proto internal/rpc/pbmeta

echo "OK: generated protobuf + grpc stubs under internal/rpc/{pb,pbmeta}"
