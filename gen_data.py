import subprocess
from pathlib import Path
import sys
import pandas as pd
import random
import csv
# -C <n> -- separate data set into <n> chunks (requires -S, default: 1)
# -f     -- force. Overwrite existing files
# -h     -- display this message
# -q     -- enable QUIET mode
# -s <n> -- set Scale Factor (SF) to  <n> (default: 1)
# -S <n> -- build the <n>th step of the data/update set (used with -C or -U)
# -U <n> -- generate <n> update sets
# -v     -- enable VERBOSE mode
#
# -b <s> -- load distributions for <s> (default: dists.dss)
# -d <n> -- split deletes between <n> files (requires -U)
# -i <n> -- split inserts between <n> files (requires -U)
# -T c   -- generate cutomers ONLY
# -T l   -- generate nation/region ONLY
# -T L   -- generate lineitem ONLY
# -T n   -- generate nation ONLY
# -T o   -- generate orders/lineitem ONLY
# -T O   -- generate orders ONLY
# -T p   -- generate parts/partsupp ONLY
# -T P   -- generate parts ONLY
# -T r   -- generate region ONLY
# -T s   -- generate suppliers ONLY
# -T S   -- generate partsupp ONLY

wq = sys.argv[1]
s = int(sys.argv[2])
S = 1

if wq == "line" or wq == "all":
    cmd = "./dbgen -s " + str(s) + " -S " + str(S) + " -C 50 -T L -v"
    process = subprocess.Popen(cmd.split(),cwd="/home/john/tpch-dbgen/data")
    output, error = process.communicate()
    cmd = "mv lineitem.tbl.1 lineitem.tbl." + str(s)
    process = subprocess.Popen(cmd.split(),cwd="/home/john/tpch-dbgen/data")
    output, error = process.communicate()

if wq == "order" or wq == "all":
    cmd = "./dbgen -s " + str(s) + " -S " + str(S) + " -C 50 -T O -v"
    process = subprocess.Popen(cmd.split(),cwd="/home/john/tpch-dbgen/data")
    output, error = process.communicate()

if wq == "supp" or wq == "all":
    cmd = "./dbgen -s " + str(s) + " -S " + str(S) + " -C 50 -T s -v"
    process = subprocess.Popen(cmd.split(),cwd="/home/john/tpch-dbgen/data")
    output, error = process.communicate()

if wq == "parts" or wq == "all":
    cmd = "./dbgen -s " + str(s) + " -S " + str(S) + " -C 50 -T S -v"
    process = subprocess.Popen(cmd.split(),cwd="/home/john/tpch-dbgen/data")
    output, error = process.communicate()
    cmd = "mv partsupp.tbl.1 partsupp.tbl." + str(s)
    process = subprocess.Popen(cmd.split(),cwd="/home/john/tpch-dbgen/data")
    output, error = process.communicate()

if wq == "nati" or wq == "all":
    cmd = "./dbgen -s " + str(s) + " -S " + str(S) + " -C 50 -T n -v"
    process = subprocess.Popen(cmd.split(),cwd="/home/john/tpch-dbgen/data")
    output, error = process.communicate()

if wq == "part" or wq == "all":
    cmd = "./dbgen -s " + str(s) + " -S " + str(S) + " -C 50 -T P -v"
    process = subprocess.Popen(cmd.split(),cwd="/home/john/tpch-dbgen/data")
    output, error = process.communicate()

if wq == "ml" or wq == "all":
    df = pd.read_csv("data/ds1.10.csv")
    maxs = df.max().values
    mins = df.min().values
    with open("/home/john/tpch-dbgen/data/ml." + str(s),"w") as ins:
        writer = csv.writer(ins, delimiter =",",lineterminator='\n')
        for rows in range(s):
            write_row = []
            for col in range(11):
                write_row.append(str('%.2f' % random.uniform(maxs[col], mins[col])))
            writer.writerow(write_row)