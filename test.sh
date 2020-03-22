#!/usr/bin/env bash
set -x
mp='x'
ls $mp
mkdir $mp/a
touch $mp/b
echo "xwdc" >> $mp/c
cat $mp/b
cat $mp/c
tree $mp
ls -l $mp
