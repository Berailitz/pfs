#!/usr/bin/env bash
set -x
mp='x'
ls $mp

mkdir $mp/a
ls $mp

echo "1stline" > $mp/a/text
cat $mp/a/text

echo "2ndline" > $mp/a/text
echo "3rdline" >> $mp/a/text
cat $mp/a/text

mkdir $mp/a/adir
ls -l $mp/a

rmdir $mp/a/adir
ls -l $mp/a

touch $mp/empty
ls -l $mp
cat $mp/empty

mkdir $mp/dir
ls -l $mp

rmdir $mp/dir
ls -l $mp

rm $mp/a/text
ls -l $mp/a

umount $mp
