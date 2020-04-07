#!/usr/bin/env bash
set -x -e

testDir="x"

echo "====BUILD START===="
go build -ldflags="-X 'main.gitCommit=\"$(git rev-list -1 --oneline --date=local HEAD)\"' -X 'main.buildTime=\"$(date '+%Y/%m/%d %H:%M:%S')\"'"
echo "====RUN START===="
./pfs -dir=$testDir
echo "====RUN END===="
