#!/usr/bin/env bash
set -x
go build -ldflags="-X 'main.gitCommit=\"$(git rev-list -1 --oneline --date=local HEAD)\"' -X 'main.buildTime=\"$(date '+%Y/%m/%d %H:%M:%S')\"'" && ./pfs -testCmd="./test.sh"
