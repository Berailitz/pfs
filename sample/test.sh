#!/usr/bin/env bash

inDir=''
outDir=''
ls='ls -l'

function usage()
{
    echo "Usage of $0:"
    echo -e "\t-h -help"
    echo -e "\t-i -inDir=inDir"
    echo -e "\t-o -outDir=outDir"
    echo ""
    echo "Test against basic filesystem operatiosn."
}

while [ "$1" != "" ]; do
    PARAM=`echo $1 | awk -F= '{print $1}'`
    VALUE=`echo $1 | awk -F= '{print $2}'`
    case $PARAM in
        -h | -help)
            usage
            exit
            ;;
        -i | --inDir)
            inDir=$VALUE
            ;;
        -o | -outDir)
            outDir=$VALUE
            ;;
        *)
            echo "ERROR: unknown parameter \"$PARAM\""
            usage
            exit 1
            ;;
    esac
    shift
done

if [ -z "$inDir" ]; then
  echo 'no inDir specified.'
  usage
  exit 1
fi
if [ -z "$outDir" ]; then
  echo 'no outDir specified.'
  usage
  exit 1
fi

echo "inDir is $inDir";
echo "outDir is $outDir";

# begin test
set -x -e

$ls $inDir
$ls $outDir

mkdir $inDir/a
$ls $outDir

echo "1stline" > $inDir/a/text
cat $outDir/a/text

echo "2ndline" > $inDir/a/text
echo "3rdline" >> $inDir/a/text
cat $outDir/a/text

mkdir $inDir/a/adir
$ls $outDir/a

rmdir $inDir/a/adir
$ls $outDir/a

touch $inDir/einDirty
$ls $outDir
cat $outDir/einDirty

mkdir $inDir/dir
$ls $outDir

rmdir $inDir/dir
$ls $outDir

rm $inDir/a/text
$ls $outDir/a

mv $inDir/einDirty $inDir/einDirty.new
$ls $outDir

mkdir $inDir/newdir
mv $inDir/einDirty.new $inDir/newdir/new.txt
$ls $outDir
$ls $outDir/newdir

mv $inDir/newdir/new.txt $inDir/back.txt
$ls $outDir
$ls $outDir/newdir

set -x

umount $inDir
umount $outDir
