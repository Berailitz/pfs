#!/usr/bin/env bash
set -x -e

testCMD="./test.sh"
testLog="./tlog.txt"

echo "====BUILD START===="
go build -ldflags="-X 'main.gitCommit=\"$(git rev-list -1 --oneline --date=local HEAD)\"' -X 'main.buildTime=\"$(date '+%Y/%m/%d %H:%M:%S')\"'"
echo "====TEST START===="
./pfs -testCmd=$testCMD -testLog=$testLog
echo "====TEST LOG===="
cat $testLog
