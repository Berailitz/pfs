#!/usr/bin/env bash
set -x
mp='x'
ls $mp
mkdir $mp/a
echo "dw" > $mp/a/z
cat $mp/a/z
touch $mp/b
echo "xwdc" >> $mp/c
cat $mp/b
cat $mp/c
tree $mp
ls -l $mp
