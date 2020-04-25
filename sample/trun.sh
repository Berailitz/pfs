#!/usr/bin/env bash
set -x -e

testCfg="sample.yaml"
testLog="./tlog.txt"

echo "====BUILD START===="
go build -ldflags="-X 'main.gitCommit=\"$(git rev-list -1 --oneline --date=local HEAD)\"' -X 'main.buildTime=\"$(date '+%Y/%m/%d %H:%M:%S')\"'"
echo "====TEST START===="
./sample -cfg="$testCfg"
echo "====TEST LOG===="
cat "$testLog"
