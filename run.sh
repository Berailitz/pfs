#!/usr/bin/env bash
set -x
go run -ldflags="-X 'main.gitCommit=\"$(git rev-list -1 --oneline --date=local HEAD)\"' -X 'main.buildTime=\"$(date '+%Y/%m/%d %H:%M:%S')\"'" main.go
