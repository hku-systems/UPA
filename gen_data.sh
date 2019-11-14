#!/bin/bash

#-C <n> -- separate data set into <n> chunks (requires -S, default: 1)
#-f     -- force. Overwrite existing files
#-h     -- display this message
#-q     -- enable QUIET mode
#-s <n> -- set Scale Factor (SF) to  <n> (default: 1)
#-S <n> -- build the <n>th step of the data/update set (used with -C or -U)
#-U <n> -- generate <n> update sets
#-v     -- enable VERBOSE mode
#
#-b <s> -- load distributions for <s> (default: dists.dss)
#-d <n> -- split deletes between <n> files (requires -U)
#-i <n> -- split inserts between <n> files (requires -U)
#-T c   -- generate cutomers ONLY
#-T l   -- generate nation/region ONLY
#-T L   -- generate lineitem ONLY
#-T n   -- generate nation ONLY
#-T o   -- generate orders/lineitem ONLY
#-T O   -- generate orders ONLY
#-T p   -- generate parts/partsupp ONLY
#-T P   -- generate parts ONLY
#-T r   -- generate region ONLY
#-T s   -- generate suppliers ONLY
#-T S   -- generate partsupp ONLY
s=1
S=1
T="L"

cd /home/john/tpch-dbgen/data

if [ ! -f lineitem* ] then
./dbgen -s $s -S $S -C 50 -T L -v
fi

if [ ! -f orders* ]; then
./dbgen -s $s -S $S -C 50 -T O -v
fi