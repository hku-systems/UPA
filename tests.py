import subprocess
import random
from pathlib import Path
import sys
import argparse

ap = argparse.ArgumentParser()
ap.add_argument("--wq", required=True,
                help="command type <1,all,upa>")
ap.add_argument("--op", required=False,
                help="operation <sing,scal>")
ap.add_argument("--sing", required=False,
                help="0 or 1")
ap.add_argument("--sp", required=False,
                help="sample size [Array]")
ap.add_argument("--db", required=False,
                help="db")
ap.add_argument("--attack", required=False,
                help="yes or no")
ap.add_argument("--path", required=False,
                help="input file path")
args = ap.parse_args()

wq = args.wq
op = args.op
sing_input = args.sing
sp = args.sp
db = args.db
#python tests.py all scal 0 [10,100,1000,10000,100000] 0
#python tests.py allc sing 10000 10,100,1000,10000 0 <- middle: samp rate
scale = [10,100,1000,10000]
scale_c = [10,100,1000,10000]

# lineitem_path = Path("/home/john/AutoDP/security.csv")
# if not lineitem_path.is_file():
f = open("security.csv","w+")
f.write("8,1,0")
f.close()

lineitem = "/home/john/tpch-dbgen/data/lineitem.tbl.original"
order = "/home/john/tpch-dbgen/data/orders.tbl.1"
supplier = "/home/john/tpch-dbgen/data/supplier.tbl.1"
partsupp = "/home/john/tpch-dbgen/data/partsupp.tbl.1"
part = "/home/john/tpch-dbgen/data/part.tbl.1"
nation = "/home/john/tpch-dbgen/data/nation.tbl"
threshold = 2
# sampleSize = sp
if wq == "simple":
    if args.attack == "yes":
        cmd1 = "./bin/spark-submit --class edu.hku.dp.e2e.simpleTest " + \
               "examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar " + \
               args.path + " " + str(sp)
        process = subprocess.Popen(cmd1,shell=True)
        output, error = process.communicate()
        print(output)

        cmd2 = "sed -i '" + str(random.randint(1,100)) + "d' " + args.path + "; ./bin/spark-submit --class edu.hku.dp.e2e.simpleTest " + \
               "examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar " + \
               args.path + " " + str(sp)
        process = subprocess.Popen(cmd2,shell=True)
        output, error = process.communicate()
        print(output)
    else:
        cmd1 = "sed -i '1d;" + str(random.randint(2,100)) + "d' " + args.path + "; ./bin/spark-submit --class edu.hku.dp.e2e.simpleTest " + \
               "examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar " + \
               args.path + " " + str(sp)
        process = subprocess.Popen(cmd1,shell=True)
        output, error = process.communicate()
        print(output)

    # python tests.py --wq simple --sp 1111
