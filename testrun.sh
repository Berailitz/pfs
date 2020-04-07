#!/usr/bin/env bash
set -x -e

testDir="x"
testCMD="./test.sh"
testLog="./tlog.txt"

echo "====BUILD START===="
go build -ldflags="-X 'main.gitCommit=\"$(git rev-list -1 --oneline --date=local HEAD)\"' -X 'main.buildTime=\"$(date '+%Y/%m/%d %H:%M:%S')\"'"
echo "====TEST START===="
./pfs -dir=$testDir -testCmd=$testCMD -testLog=$testLog
echo "====TEST LOG===="
cat $testLog
