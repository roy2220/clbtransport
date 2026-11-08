#!/usr/bin/env bash
set -euxo pipefail

cd "$(dirname "$0")"

go mod tidy
go vet ./...
go test -race ./...
bash ./.badges/generate-coverage.bash

git diff HEAD --exit-code
