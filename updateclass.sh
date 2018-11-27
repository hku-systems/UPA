#!/bin/bash
input=$1
relativePath=${input:42:${#input}-42}
#relativePath=${input:41:1}
echo $relativePath
#scp -P 2204 $1 john@127.0.0.1:/home/john/privatebox/kakute/$relativePath
scp -P 2204 $1 john@127.0.0.1:/home/john/spark-2.2.0/$relativePath
