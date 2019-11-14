import subprocess
from pathlib import Path
import sys

wq = int(sys.argv[1])
sp = int(sys.argv[2])

lineitem_path = Path("/home/john/AutoDP/security.csv")
if not lineitem_path.is_file():
    f = open("security.csv","w+")
    f.write("10,1")
    f.close()

lineitem = "/home/john/tpch-dbgen/data/lineitem.tbl*"
order = "/home/john/tpch-dbgen/data/orders.tbl*"
supplier = "/home/john/tpch-dbgen/data/supplier.tbl*"
partsupp = "/home/john/tpch-dbgen/data/partsupp.tbl*"
part = "/home/john/tpch-dbgen/data/part.tbl*"
nation = "/home/john/tpch-dbgen/data/nation.tbl*"
threshold = 2
sampleSize = sp

if wq == 1:
    output1 = open("output1.txt","w+")
    cmd1 = "./bin/spark-submit --class edu.hku.dp.TPCH1DP examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar " + \
        lineitem + " " + lineitem + "  "+ str(threshold) + " " + str(sampleSize)
    process = subprocess.Popen(cmd1.split(), stdout=output1)
    output, error = process.communicate()

if wq == 4:
    output4 = open("output4.txt","w+")
    cmd1 = "./bin/spark-submit --class edu.hku.dp.TPCH4DP examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar " + \
        order + " " + order + "  " + lineitem + " " + lineitem + " " + str(threshold) + " " + str(sampleSize)
    process = subprocess.Popen(cmd1.split(), stdout=output4)
    output, error = process.communicate()

if wq == 6:
    output6 = open("output6.txt","w+")
    cmd1 = "./bin/spark-submit --class edu.hku.dp.TPCH6DP examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar " + \
        lineitem + " " + lineitem + " " + str(threshold) + " " + str(sampleSize)
    process = subprocess.Popen(cmd1.split(), stdout=output6)
    output, error = process.communicate()

if wq == 11:
    output11 = open("output11.txt","w+")
    cmd1 = "./bin/spark-submit --class edu.hku.dp.TPCH11DP examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar " + \
        supplier + " " + supplier + " " + nation + " " + partsupp + \
         " " + partsupp + " " + str(threshold) + " " + str(sampleSize)
    process = subprocess.Popen(cmd1.split(), stdout=output11)
    output, error = process.communicate()

if wq == 13:
    output13 = open("output13.txt","w+")
    cmd1 = "./bin/spark-submit --class edu.hku.dp.TPCH13DP examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar " + \
        order + " " + order + " " + lineitem + " " + lineitem + \
        " " + str(threshold) + " " + str(sampleSize)
    process = subprocess.Popen(cmd1.split(), stdout=output13)
    output, error = process.communicate()

if wq == 16:
    output16 = open("output16.txt","w+")
    cmd1 = "./bin/spark-submit --class edu.hku.dp.TPCH16DP examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar " + \
        part + " " + part + " " + supplier + " " + supplier + \
        " " + partsupp + " " + partsupp + " " + \
         str(threshold) + " " + str(sampleSize)
    process = subprocess.Popen(cmd1.split(), stdout=output16)
    output, error = process.communicate()

if wq == 21:
    output21 = open("output21.txt","w+")
    cmd1 = "./bin/spark-submit --class edu.hku.dp.TPCH21DP examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar " + \
        supplier + " " + supplier + " " + lineitem + " " + lineitem + \
        " " + order + " " + order + " " + nation + " " +\
         str(threshold) + " " + str(sampleSize)
    process = subprocess.Popen(cmd1.split(), stdout=output21)
    output, error = process.communicate()