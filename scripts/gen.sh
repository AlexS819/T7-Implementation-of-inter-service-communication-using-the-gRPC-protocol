#!/usr/bin/env bash
# Генерація Go-коду з контракту proto/media.proto (bash).
# Потрібні: protoc, protoc-gen-go, protoc-gen-go-grpc у PATH.
set -euo pipefail

echo "Generating Go code from proto/media.proto..."
protoc --go_out=. --go_opt=paths=source_relative \
       --go-grpc_out=. --go-grpc_opt=paths=source_relative \
       proto/media.proto
echo "Done: proto/media.pb.go, proto/media_grpc.pb.go"
