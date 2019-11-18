import subprocess
from pathlib import Path
import sys

wq = sys.argv[1]
op = sys.argv[2]
sing_input = int(sys.argv[3])
sing = sing_input if sing_input > 0 else 1
sp = int(sys.argv[4])
db = sys.argv[5]

lineitem_path = Path("/home/john/AutoDP/security.csv")
if not lineitem_path.is_file():
    f = open("security.csv","w+")
    f.write("10,1")
    f.close()

lineitem = "/home/john/tpch-dbgen/data/lineitem.tbl.1"
order = "/home/john/tpch-dbgen/data/orders.tbl.1"
supplier = "/home/john/tpch-dbgen/data/supplier.tbl.1"
partsupp = "/home/john/tpch-dbgen/data/partsupp.tbl.1"
part = "/home/john/tpch-dbgen/data/part.tbl.1"
nation = "/home/john/tpch-dbgen/data/nation.tbl"
threshold = 2
sampleSize = sp

if wq == "1" or wq == "all":
    output1 = open("output1.txt","w+")
    err1 = open("err1.txt","w+")
    cmd1 = "./bin/spark-submit --master spark://10.22.1.3:7081 --class edu.hku.dp.TPCH1DP examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar " + \
       lineitem + " " + lineitem + "  "+ str(threshold) + " " + str(sampleSize)
    process = subprocess.Popen(cmd1.split(), stdout=output1, stderr=err1)
    output, error = process.communicate()

    #./bin/spark-submit --master spark://10.22.1.3:7081 --class edu.hku.dp.TPCH1DP --conf "spark.executor.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps" --conf "spark.driver.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps" examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar /home/john/tpch-dbgen/data/lineitem.tbl.1 /home/john/tpch-dbgen/data/lineitem.tbl.1 2 30
    #

if wq == "4" or wq == "all":
    output4 = open("output4.txt","w+")
    err4 = open("err4.txt","w+")
    cmd1 = "./bin/spark-submit --master spark://10.22.1.3:7081 --class edu.hku.dp.TPCH4DP examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar " + \
           order + " " + order + "  " + lineitem + " " + lineitem + " " + str(threshold) + " " + str(sampleSize)
    process = subprocess.Popen(cmd1.split(), stdout=output4, stderr=err4)
    output, error = process.communicate()

if wq == "6" or wq == "all":
    output6 = open("output6.txt","w+")
    err6 = open("err6.txt","w+")
    cmd1 = "./bin/spark-submit --master spark://10.22.1.3:7081 --class edu.hku.dp.TPCH6DP examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar " + \
           lineitem + " " + lineitem + " " + str(threshold) + " " + str(sampleSize)
    process = subprocess.Popen(cmd1.split(), stdout=output6, stderr=err6)
    output, error = process.communicate()

if wq == "11" or wq == "all":
    output11 = open("output11.txt","w+")
    err11 = open("err11.txt","w+")
    cmd1 = "./bin/spark-submit --master spark://10.22.1.3:7081 --class edu.hku.dp.TPCH11DP examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar " + \
           supplier + " " + supplier + " " + nation + " " + partsupp + \
           " " + partsupp + " " + str(threshold) + " " + str(sampleSize)
    process = subprocess.Popen(cmd1.split(), stdout=output11, stderr=err11)
    output, error = process.communicate()

if wq == "13" or wq == "all":
    output13 = open("output13.txt","w+")
    err13 = open("err13.txt","w+")
    cmd1 = "./bin/spark-submit --master spark://10.22.1.3:7081 --class edu.hku.dp.TPCH13DP examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar " + \
           lineitem + " " + lineitem + " " + order + " " + order + \
           " " + str(threshold) + " " + str(sampleSize)
    process = subprocess.Popen(cmd1.split(), stdout=output13, stderr=err13)
    output, error = process.communicate()

if wq == "16" or wq == "all":
    output16 = open("output16.txt","w+")
    err16 = open("err16.txt","w+")
    cmd1 = "./bin/spark-submit --master spark://10.22.1.3:7081 --class edu.hku.dp.TPCH16DP examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar " + \
           part + " " + part + " " + supplier + " " + supplier + \
           " " + partsupp + " " + partsupp + " " + \
           str(threshold) + " " + str(sampleSize)
    process = subprocess.Popen(cmd1.split(), stdout=output16, stderr=err16)
    output, error = process.communicate()

if wq == "21" or wq == "all":
    if op == "sing":
        lineitem = "/home/john/tpch-dbgen/data/lineitem.tbl." + str(sing)
        output21 = open("output21-" + str(sing) + ".txt","w+")
        err21 = open("err21-" + str(sing) + ".txt","w+")
        cmd1 = "./bin/spark-submit --master spark://10.22.1.3:7081 --class edu.hku.dp.TPCH21DP " + \
               "--conf 'spark.executor.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
               "--conf 'spark.driver.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
               "--driver-memory 50g --executor-memory 50g --conf spark.executor.extraJavaOptions='-Xms50g' --conf spark.driver.extraJavaOptions='-Xms50g' " + \
               "examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar " + \
               supplier + " " + supplier + " " + lineitem + " " + "/home/john/tpch-dbgen/data/lineitem.tbl.1" + \
               " " + order + " " + order + " " + nation + " " + \
               str(threshold) + " " + str(sampleSize) + " " + db
        #./bin/spark-submit --master spark://10.22.1.3:7081 --class edu.hku.dp.TPCH21DP --conf "spark.executor.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps" --conf "spark.driver.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps" examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar
        process = subprocess.Popen(cmd1,shell=True, stdout=output21, stderr=err21)
        output, error = process.communicate()
    elif op == "scal":
        for i in [10,30,50,70,100]:
            lineitem = "/home/john/tpch-dbgen/data/lineitem.tbl." + str(i)
            output21 = open("output21-" + str(i) + ".txt","w+")
            err21 = open("err21-" + str(i) + ".txt","w+")
            cmd1 = "./bin/spark-submit --master spark://10.22.1.3:7081 --class edu.hku.dp.TPCH21DP examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar " + \
               supplier + " " + supplier + " " + lineitem + " " + "/home/john/tpch-dbgen/data/lineitem.tbl.1" + \
               " " + order + " " + order + " " + nation + " " + \
               str(threshold) + " " + str(sampleSize) + " " + db
            process = subprocess.Popen(cmd1.split(), stdout=output21, stderr=err21)
            output, error = process.communicate()

if wq == "1o" or wq == "all":
    output1 = open("output1o.txt","w+")
    err1 = open("err1o.txt","w+")
    cmd1 = "./bin/spark-submit --master spark://10.22.1.3:7081 --class edu.hku.dp.TPCH1 examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar " + \
           lineitem
    process = subprocess.Popen(cmd1.split(), stdout=output1, stderr=err1)
    output, error = process.communicate()

if wq == "4o" or wq == "all":
    output4 = open("output4o.txt","w+")
    err4 = open("err4o.txt","w+")
    cmd1 = "./bin/spark-submit --master spark://10.22.1.3:7081 --class edu.hku.dp.TPCH4 examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar " + \
           order + " " + lineitem
    process = subprocess.Popen(cmd1.split(), stdout=output4, stderr=err4)
    output, error = process.communicate()

if wq == "6o" or wq == "all":
    output6 = open("output6o.txt","w+")
    err6 = open("err6o.txt","w+")
    cmd1 = "./bin/spark-submit --master spark://10.22.1.3:7081 --class edu.hku.dp.TPCH6 examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar " + \
           lineitem
    process = subprocess.Popen(cmd1.split(), stdout=output6, stderr=err6)
    output, error = process.communicate()

if wq == "11o" or wq == "all":
    output11 = open("output11o.txt","w+")
    err11 = open("err11o.txt","w+")
    cmd1 = "./bin/spark-submit --master spark://10.22.1.3:7081 --class edu.hku.dp.TPCH11 examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar " + \
           supplier + " " + nation + " " + partsupp
    process = subprocess.Popen(cmd1.split(), stdout=output11, stderr=err11)
    output, error = process.communicate()

if wq == "13o" or wq == "all":
    output13 = open("output13o.txt","w+")
    err13 = open("err13o.txt","w+")
    cmd1 = "./bin/spark-submit --master spark://10.22.1.3:7081 --class edu.hku.dp.TPCH13 examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar " + \
           lineitem + " " + order
    process = subprocess.Popen(cmd1.split(), stdout=output13, stderr=err13)
    output, error = process.communicate()

if wq == "21o" or wq == "all":
    if op == "sing":
        lineitem = "/home/john/tpch-dbgen/data/lineitem.tbl." + str(sing)
        output21 = open("output21o-" + str(sing) + ".txt","w+")
        err21 = open("err21o-" + str(sing) + ".txt","w+")
        cmd1 = "./bin/spark-submit --master spark://10.22.1.3:7081 --class edu.hku.dp.TPCH21 examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar " + \
               " " + supplier + " " + lineitem + " " + "/home/john/tpch-dbgen/data/lineitem.tbl.1" + \
               " " + order + " " + nation + " " + db
        process = subprocess.Popen(cmd1.split(), stdout=output21, stderr=err21)
        output, error = process.communicate()
    elif op == "scal":
        for i in [10,30,50,70,100]:
            lineitem = "/home/john/tpch-dbgen/data/lineitem.tbl." + str(i)
            output21 = open("output21o-" + str(i) + ".txt","w+")
            err21 = open("err21o-" + str(i) + ".txt","w+")
            cmd1 = "./bin/spark-submit --master spark://10.22.1.3:7081 --class edu.hku.dp.TPCH21 examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar " + \
                   " " + supplier + " " + lineitem + " " + "/home/john/tpch-dbgen/data/lineitem.tbl.1" + \
                   " " + order + " " + nation + " " + db
            process = subprocess.Popen(cmd1.split(), stdout=output21, stderr=err21)
            output, error = process.communicate()
