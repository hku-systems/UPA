import subprocess
from pathlib import Path
import sys

wq = sys.argv[1]
op = sys.argv[2]
sing_input = int(sys.argv[3])
sing = sing_input if sing_input > 0 else 1
sp = list(sys.argv[4].split(','))
db = sys.argv[5]
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
for sampleSize in sp:
    if wq == "1" or wq == "all" or wq == "upa":
        if op == "sing":
            lineitem = "/home/john/tpch-dbgen/data/lineitem.tbl." + str(sing)
            output1 = open("output1-" + str(sing) + "," + str(sampleSize) + ".txt","w+")
            err1 = open("err1-" + str(sing) + "," + str(sampleSize) + ".txt","w+")
            cmd1 = "./bin/spark-submit --master spark://10.22.1.3:7081 --class edu.hku.dp.TPCH1DP " + \
                   "--conf 'spark.executor.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
                   "--conf 'spark.driver.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
                   "--driver-memory 50g --executor-memory 50g --conf spark.executor.extraJavaOptions='-Xms50g' --conf spark.driver.extraJavaOptions='-Xms50g' " + \
                   "examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar " + \
                   lineitem + " " + lineitem + "  "+ str(threshold) + " " + str(sampleSize)
            process = subprocess.Popen(cmd1,shell=True, stdout=output1, stderr=err1)
            output, error = process.communicate()
        elif op == "scal":
            for i in scale:
                lineitem = "/home/john/tpch-dbgen/data/lineitem.tbl." + str(i)
                output1 = open("output1-" + str(i) + "," + str(sampleSize) + ".txt","w+")
                err1 = open("err1-" + str(i) + "," + str(sampleSize) + ".txt","w+")
                cmd1 = "./bin/spark-submit --master spark://10.22.1.3:7081 --class edu.hku.dp.TPCH1DP " + \
                       "--conf 'spark.executor.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
                       "--conf 'spark.driver.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
                       "--driver-memory 50g --executor-memory 50g --conf spark.executor.extraJavaOptions='-Xms50g' --conf spark.driver.extraJavaOptions='-Xms50g' " + \
                       "examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar " + \
                       lineitem + " " + lineitem + "  "+ str(threshold) + " " + str(sampleSize)
                process = subprocess.Popen(cmd1,shell=True, stdout=output1, stderr=err1)
                output, error = process.communicate()

        #./bin/spark-submit --master spark://10.22.1.3:7081 --class edu.hku.dp.TPCH1DP --conf "spark.executor.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps" --conf "spark.driver.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps" examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar /home/john/tpch-dbgen/data/lineitem.tbl.original /home/john/tpch-dbgen/data/lineitem.tbl.original 2 30
        #

    if wq == "4" or wq == "all" or wq == "upa":
        if op == "sing":
            lineitem = "/home/john/tpch-dbgen/data/lineitem.tbl." + str(sing)
            output4 = open("output4-" + str(sing) + "," + str(sampleSize) + ".txt","w+")
            err4 = open("err4-" + str(sing) + "," + str(sampleSize) + ".txt","w+")
            cmd1 = "./bin/spark-submit --master spark://10.22.1.3:7081 --class edu.hku.dp.TPCH4DP " + \
                   "--conf 'spark.executor.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
                   "--conf 'spark.driver.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
                   "--driver-memory 50g --executor-memory 50g --conf spark.executor.extraJavaOptions='-Xms50g' --conf spark.driver.extraJavaOptions='-Xms50g' " + \
                   "examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar " + \
                   order + " " + order + "  " + lineitem + " " + lineitem + " " + str(threshold) + " " + str(sampleSize)
            process = subprocess.Popen(cmd1,shell=True, stdout=output4, stderr=err4)
            output, error = process.communicate()
        elif op == "scal":
            for i in scale:
                lineitem = "/home/john/tpch-dbgen/data/lineitem.tbl." + str(i)
                output4 = open("output4-" + str(i) + "," + str(sampleSize) + ".txt","w+")
                err4 = open("err4-" + str(i) + "," + str(sampleSize) + ".txt","w+")
                cmd1 = "./bin/spark-submit --master spark://10.22.1.3:7081 --class edu.hku.dp.TPCH4DP " + \
                       "--conf 'spark.executor.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
                       "--conf 'spark.driver.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
                       "--driver-memory 50g --executor-memory 50g --conf spark.executor.extraJavaOptions='-Xms50g' --conf spark.driver.extraJavaOptions='-Xms50g' " + \
                       "examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar " + \
                       order + " " + order + "  " + lineitem + " " + lineitem + " " + str(threshold) + " " + str(sampleSize)
                process = subprocess.Popen(cmd1,shell=True, stdout=output4, stderr=err4)
                output, error = process.communicate()

    if wq == "6" or wq == "all" or wq == "upa":
        if op == "sing":
            lineitem = "/home/john/tpch-dbgen/data/lineitem.tbl." + str(sing)
            output6 = open("output6-" + str(sing) + "," + str(sampleSize) + ".txt","w+")
            err6 = open("err6-" + str(sing) + "," + str(sampleSize) + ".txt","w+")
            cmd1 = "./bin/spark-submit --master spark://10.22.1.3:7081 --class edu.hku.dp.TPCH6DP " + \
                   "--conf 'spark.executor.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
                   "--conf 'spark.driver.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
                   "--driver-memory 50g --executor-memory 50g --conf spark.executor.extraJavaOptions='-Xms50g' --conf spark.driver.extraJavaOptions='-Xms50g' " + \
                   "examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar " + \
                   lineitem + " " + lineitem + " " + str(threshold) + " " + str(sampleSize)
            process = subprocess.Popen(cmd1,shell=True, stdout=output6, stderr=err6)
            output, error = process.communicate()
        elif op == "scal":
            for i in scale:
                lineitem = "/home/john/tpch-dbgen/data/lineitem.tbl." + str(i)
                output6 = open("output6-" + str(i) + "," + str(sampleSize) + ".txt","w+")
                err6 = open("err6-" + str(i) + "," + str(sampleSize) + ".txt","w+")
                cmd1 = "./bin/spark-submit --master spark://10.22.1.3:7081 --class edu.hku.dp.TPCH6DP " + \
                       "--conf 'spark.executor.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
                       "--conf 'spark.driver.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
                       "--driver-memory 50g --executor-memory 50g --conf spark.executor.extraJavaOptions='-Xms50g' --conf spark.driver.extraJavaOptions='-Xms50g' " + \
                       "examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar " + \
                       lineitem + " " + lineitem + " " + str(threshold) + " " + str(sampleSize)
                process = subprocess.Popen(cmd1,shell=True, stdout=output6, stderr=err6)
                output, error = process.communicate()

    if wq == "11" or wq == "all" or wq == "upa":
        if op == "sing":
            partsupp = "/home/john/tpch-dbgen/data/partsupp.tbl." + str(sing)
            output11 = open("output11-" + str(sing) + "," + str(sampleSize) + ".txt","w+")
            err11 = open("err11-" + str(sing) + "," + str(sampleSize) + ".txt","w+")
            cmd1 = "./bin/spark-submit --master spark://10.22.1.3:7081 --class edu.hku.dp.TPCH11DP " + \
                   "--conf 'spark.executor.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
                   "--conf 'spark.driver.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
                   "--driver-memory 50g --executor-memory 50g --conf spark.executor.extraJavaOptions='-Xms50g' --conf spark.driver.extraJavaOptions='-Xms50g' " + \
                   "examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar " + \
                   supplier + " " + supplier + " " + nation + " " + partsupp + \
                   " " + partsupp + " " + str(threshold) + " " + str(sampleSize)
            process = subprocess.Popen(cmd1,shell=True, stdout=output11, stderr=err11)
            output, error = process.communicate()
        elif op == "scal":
            for i in scale:
                partsupp = "/home/john/tpch-dbgen/data/partsupp.tbl." + str(i)
                output11 = open("output11-" + str(i) + "," + str(sampleSize) + ".txt","w+")
                err11 = open("err11-" + str(i) + "," + str(sampleSize) + ".txt","w+")
                cmd1 = "./bin/spark-submit --master spark://10.22.1.3:7081 --class edu.hku.dp.TPCH11DP " + \
                       "--conf 'spark.executor.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
                       "--conf 'spark.driver.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
                       "--driver-memory 50g --executor-memory 50g --conf spark.executor.extraJavaOptions='-Xms50g' --conf spark.driver.extraJavaOptions='-Xms50g' " + \
                       "examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar " + \
                       supplier + " " + supplier + " " + nation + " " + partsupp + \
                       " " + partsupp + " " + str(threshold) + " " + str(sampleSize)
                process = subprocess.Popen(cmd1,shell=True, stdout=output11, stderr=err11)
                output, error = process.communicate()

    if wq == "13" or wq == "all" or wq == "upa":
        if op == "sing":
            lineitem = "/home/john/tpch-dbgen/data/lineitem.tbl." + str(sing)
            output13 = open("output13-" + str(sing) + "," + str(sampleSize) + ".txt","w+")
            err13 = open("err13-" + str(sing) + "," + str(sampleSize) + ".txt","w+")
            cmd1 = "./bin/spark-submit --master spark://10.22.1.3:7081 --class edu.hku.dp.TPCH13DP " + \
                   "--conf 'spark.executor.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
                   "--conf 'spark.driver.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
                   "--driver-memory 50g --executor-memory 50g --conf spark.executor.extraJavaOptions='-Xms50g' --conf spark.driver.extraJavaOptions='-Xms50g' " + \
                   "examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar " + \
                   lineitem + " " + lineitem + " " + order + " " + order + \
                   " " + str(threshold) + " " + str(sampleSize)
            process = subprocess.Popen(cmd1,shell=True, stdout=output13, stderr=err13)
            output, error = process.communicate()
        elif op == "scal":
            for i in scale:
                lineitem = "/home/john/tpch-dbgen/data/lineitem.tbl." + str(i)
                output13 = open("output13-" + str(i) + "," + str(sampleSize) + ".txt","w+")
                err13 = open("err13-" + str(i) + "," + str(sampleSize) + ".txt","w+")
                cmd1 = "./bin/spark-submit --master spark://10.22.1.3:7081 --class edu.hku.dp.TPCH13DP " + \
                       "--conf 'spark.executor.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
                       "--conf 'spark.driver.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
                       "--driver-memory 50g --executor-memory 50g --conf spark.executor.extraJavaOptions='-Xms50g' --conf spark.driver.extraJavaOptions='-Xms50g' " + \
                       "examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar " + \
                       lineitem + " " + lineitem + " " + order + " " + order + \
                       " " + str(threshold) + " " + str(sampleSize)
                process = subprocess.Popen(cmd1,shell=True, stdout=output13, stderr=err13)
                output, error = process.communicate()

    if wq == "16" or wq == "all" or wq == "upa":
        if op == "sing":
            partsupp = "/home/john/tpch-dbgen/data/partsupp.tbl." + str(sing)
            output16 = open("output16-" + str(sing) + "," + str(sampleSize) + ".txt","w+")
            err16 = open("err16-" + str(sing) + "," + str(sampleSize) + ".txt","w+")
            cmd1 = "./bin/spark-submit --master spark://10.22.1.3:7081 --class edu.hku.dp.TPCH16DP " + \
                   "--conf 'spark.executor.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
                   "--conf 'spark.driver.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
                   "--driver-memory 50g --executor-memory 50g --conf spark.executor.extraJavaOptions='-Xms50g' --conf spark.driver.extraJavaOptions='-Xms50g' " + \
                   "examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar " + \
                   part + " " + part + " " + supplier + " " + supplier + \
                   " " + partsupp + " " + partsupp + " " + \
                   str(threshold) + " " + str(sampleSize)
            process = subprocess.Popen(cmd1,shell=True, stdout=output16, stderr=err16)
            output, error = process.communicate()
        elif op == "scal":
            for i in scale:
                partsupp = "/home/john/tpch-dbgen/data/partsupp.tbl." + str(i)
                output16 = open("output16-" + str(i) + "," + str(sampleSize) + ".txt","w+")
                err16 = open("err16-" + str(i) + "," + str(sampleSize) + ".txt","w+")
                cmd1 = "./bin/spark-submit --master spark://10.22.1.3:7081 --class edu.hku.dp.TPCH16DP " + \
                       "--conf 'spark.executor.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
                       "--conf 'spark.driver.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
                       "--driver-memory 50g --executor-memory 50g --conf spark.executor.extraJavaOptions='-Xms50g' --conf spark.driver.extraJavaOptions='-Xms50g' " + \
                       "examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar " + \
                       part + " " + part + " " + supplier + " " + supplier + \
                       " " + partsupp + " " + partsupp + " " + \
                       str(threshold) + " " + str(sampleSize)
                process = subprocess.Popen(cmd1,shell=True, stdout=output16, stderr=err16)
                output, error = process.communicate()

    if wq == "21" or wq == "all" or wq == "upa":
        if op == "sing":
            lineitem = "/home/john/tpch-dbgen/data/lineitem.tbl." + str(sing)
            output21 = open("output21-" + str(sing) + "," + str(sampleSize) + ".txt","w+")
            err21 = open("err21-" + str(sing) + "," + str(sampleSize) + ".txt","w+")
            cmd1 = "./bin/spark-submit --master spark://10.22.1.3:7081 --class edu.hku.dp.TPCH21DP " + \
                   "--conf 'spark.executor.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
                   "--conf 'spark.driver.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
                   "--driver-memory 50g --executor-memory 50g --conf spark.executor.extraJavaOptions='-Xms50g' --conf spark.driver.extraJavaOptions='-Xms50g' " + \
                   "examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar " + \
                   supplier + " " + supplier + " " + lineitem + " " + "/home/john/tpch-dbgen/data/lineitem.tbl.original" + \
                   " " + order + " " + order + " " + nation + " " + \
                   str(threshold) + " " + str(sampleSize) + " " + db
            #./bin/spark-submit --master spark://10.22.1.3:7081 --class edu.hku.dp.TPCH21DP --conf "spark.executor.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps" --conf "spark.driver.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps" examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar
            process = subprocess.Popen(cmd1,shell=True, stdout=output21, stderr=err21)
            output, error = process.communicate()
        elif op == "scal":
            for i in scale:
                lineitem = "/home/john/tpch-dbgen/data/lineitem.tbl." + str(i)
                output21 = open("output21-" + str(i) + "," + str(sampleSize) + ".txt","w+")
                err21 = open("err21-" + str(i) + "," + str(sampleSize) + ".txt","w+")
                cmd1 = "./bin/spark-submit --master spark://10.22.1.3:7081 --class edu.hku.dp.TPCH21DP " + \
                       "--conf 'spark.executor.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
                       "--conf 'spark.driver.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
                       "--driver-memory 50g --executor-memory 50g --conf spark.executor.extraJavaOptions='-Xms50g' --conf spark.driver.extraJavaOptions='-Xms50g' " + \
                       "examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar " + \
                       supplier + " " + supplier + " " + lineitem + " " + "/home/john/tpch-dbgen/data/lineitem.tbl.original" + \
                       " " + order + " " + order + " " + nation + " " + \
                       str(threshold) + " " + str(sampleSize) + " " + db
                process = subprocess.Popen(cmd1,shell=True, stdout=output21, stderr=err21)
                output, error = process.communicate()

    if wq == "lr" or wq == "all" or wq == "upa":
        if op == "sing":
            ml_data = "/home/john/tpch-dbgen/data/ml." + str(sing)
            output21 = open("outputlr-" + str(sing) + "," + str(sampleSize) + ".txt","w+")
            err21 = open("errlr-" + str(sing) + "," + str(sampleSize) + ".txt","w+")
            cmd1 = "./bin/spark-submit --master spark://10.22.1.3:7081 --class edu.hku.dp.SparkHdfsLRDP " + \
                   "--conf 'spark.executor.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
                   "--conf 'spark.driver.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
                   "--driver-memory 50g --executor-memory 50g --conf spark.executor.extraJavaOptions='-Xms50g' --conf spark.driver.extraJavaOptions='-Xms50g' " + \
                   "examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar " + \
                   ml_data + " 1 9 " + \
                   str(threshold) + " " + str(sampleSize)
            #./bin/spark-submit --master spark://10.22.1.3:7081 --class edu.hku.dp.TPCH21DP --conf "spark.executor.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps" --conf "spark.driver.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps" examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar
            process = subprocess.Popen(cmd1,shell=True, stdout=output21, stderr=err21)
            output, error = process.communicate()
        elif op == "scal":
            for i in scale:
                ml_data = "/home/john/tpch-dbgen/data/ml." + str(i)
                output21 = open("outputlr-" + str(i) + "," + str(sampleSize) + ".txt","w+")
                err21 = open("errlr-" + str(i) + "," + str(sampleSize) + ".txt","w+")
                cmd1 = "./bin/spark-submit --master spark://10.22.1.3:7081 --class edu.hku.dp.SparkHdfsLRDP " + \
                       "--conf 'spark.executor.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
                       "--conf 'spark.driver.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
                       "--driver-memory 50g --executor-memory 50g --conf spark.executor.extraJavaOptions='-Xms50g' --conf spark.driver.extraJavaOptions='-Xms50g' " + \
                       "examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar " + \
                       ml_data + " 1 9 " + \
                       str(threshold) + " " + str(sampleSize)
                process = subprocess.Popen(cmd1,shell=True, stdout=output21, stderr=err21)
                output, error = process.communicate()

    if wq == "km" or wq == "all" or wq == "upa":
        if op == "sing":
            ml_data = "/home/john/tpch-dbgen/data/ml." + str(sing)
            output21 = open("outputkm-" + str(sing) + "," + str(sampleSize) + ".txt","w+")
            err21 = open("errkm-" + str(sing) + "," + str(sampleSize) + ".txt","w+")
            cmd1 = "./bin/spark-submit --master spark://10.22.1.3:7081 --class edu.hku.dp.SparkKMeansDP " + \
                   "--conf 'spark.executor.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
                   "--conf 'spark.driver.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
                   "--driver-memory 50g --executor-memory 50g --conf spark.executor.extraJavaOptions='-Xms50g' --conf spark.driver.extraJavaOptions='-Xms50g' " + \
                   "examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar " + \
                   ml_data + " 1 3 10 " + \
                   str(threshold) + " " + str(sampleSize)
            #./bin/spark-submit --master spark://10.22.1.3:7081 --class edu.hku.dp.TPCH21DP --conf "spark.executor.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps" --conf "spark.driver.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps" examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar
            process = subprocess.Popen(cmd1,shell=True, stdout=output21, stderr=err21)
            output, error = process.communicate()
        elif op == "scal":
            for i in scale:
                ml_data = "/home/john/tpch-dbgen/data/ml." + str(i)
                outputkm = open("outputkm-" + str(i) + "," + str(sampleSize) + ".txt","w+")
                errkm = open("errkm-" + str(i) + "," + str(sampleSize) + ".txt","w+")
                cmd1 = "./bin/spark-submit --master spark://10.22.1.3:7081 --class edu.hku.dp.SparkKMeansDP " + \
                       "--conf 'spark.executor.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
                       "--conf 'spark.driver.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
                       "--driver-memory 50g --executor-memory 50g --conf spark.executor.extraJavaOptions='-Xms50g' --conf spark.driver.extraJavaOptions='-Xms50g' " + \
                       "examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar " + \
                       ml_data + " 1 3 10 " + \
                       str(threshold) + " " + str(sampleSize)
                process = subprocess.Popen(cmd1,shell=True, stdout=outputkm, stderr=errkm)
                output, error = process.communicate()

if wq == "1o" or wq == "all":
    if op == "sing":
        lineitem = "/home/john/tpch-dbgen/data/lineitem.tbl." + str(sing)
        output1 = open("output1o-" + str(sing) + ".txt","w+")
        err1 = open("err1o-" + str(sing) + ".txt","w+")
        cmd1 = "./bin/spark-submit --master spark://10.22.1.3:7081 --class edu.hku.dp.TPCH1 " + \
               "--conf 'spark.executor.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
               "--conf 'spark.driver.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
               "--driver-memory 50g --executor-memory 50g --conf spark.executor.extraJavaOptions='-Xms50g' --conf spark.driver.extraJavaOptions='-Xms50g' " + \
               "examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar " + \
               lineitem
        process = subprocess.Popen(cmd1,shell=True, stdout=output1, stderr=err1)
        output, error = process.communicate()
    elif op == "scal":
        for i in scale:
            lineitem = "/home/john/tpch-dbgen/data/lineitem.tbl." + str(i)
            output1 = open("output1o-" + str(i) + ".txt","w+")
            err1 = open("err1o-" + str(i) + ".txt","w+")
            cmd1 = "./bin/spark-submit --master spark://10.22.1.3:7081 --class edu.hku.dp.TPCH1 " + \
                   "--conf 'spark.executor.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
                   "--conf 'spark.driver.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
                   "--driver-memory 50g --executor-memory 50g --conf spark.executor.extraJavaOptions='-Xms50g' --conf spark.driver.extraJavaOptions='-Xms50g' " + \
                   "examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar " + \
                   lineitem
            process = subprocess.Popen(cmd1,shell=True, stdout=output1, stderr=err1)
            output, error = process.communicate()

if wq == "4o" or wq == "all":
    if op == "sing":
        lineitem = "/home/john/tpch-dbgen/data/lineitem.tbl." + str(sing)
        output4 = open("output4o-" + str(sing) + ".txt","w+")
        err4 = open("err4o-" + str(sing) + ".txt","w+")
        cmd1 = "./bin/spark-submit --master spark://10.22.1.3:7081 --class edu.hku.dp.TPCH4 " + \
               "--conf 'spark.executor.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
               "--conf 'spark.driver.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
               "--driver-memory 50g --executor-memory 50g --conf spark.executor.extraJavaOptions='-Xms50g' --conf spark.driver.extraJavaOptions='-Xms50g' " + \
               "examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar " + \
               order + "  " + lineitem
        process = subprocess.Popen(cmd1,shell=True, stdout=output4, stderr=err4)
        output, error = process.communicate()
    elif op == "scal":
        for i in scale:
            lineitem = "/home/john/tpch-dbgen/data/lineitem.tbl." + str(i)
            output4 = open("output4o-" + str(i) + ".txt","w+")
            err4 = open("err4o-" + str(i) + ".txt","w+")
            cmd1 = "./bin/spark-submit --master spark://10.22.1.3:7081 --class edu.hku.dp.TPCH4 " + \
                   "--conf 'spark.executor.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
                   "--conf 'spark.driver.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
                   "--driver-memory 50g --executor-memory 50g --conf spark.executor.extraJavaOptions='-Xms50g' --conf spark.driver.extraJavaOptions='-Xms50g' " + \
                   "examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar " + \
                   order + "  " + lineitem
            process = subprocess.Popen(cmd1,shell=True, stdout=output4, stderr=err4)
            output, error = process.communicate()

if wq == "6o" or wq == "all":
    if op == "sing":
        lineitem = "/home/john/tpch-dbgen/data/lineitem.tbl." + str(sing)
        output6 = open("output6o-" + str(sing) + ".txt","w+")
        err6 = open("err6o-" + str(sing) + ".txt","w+")
        cmd1 = "./bin/spark-submit --master spark://10.22.1.3:7081 --class edu.hku.dp.original.TPCH6 " + \
               "--conf 'spark.executor.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
               "--conf 'spark.driver.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
               "--driver-memory 50g --executor-memory 50g --conf spark.executor.extraJavaOptions='-Xms50g' --conf spark.driver.extraJavaOptions='-Xms50g' " + \
               "examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar " + \
               lineitem
        process = subprocess.Popen(cmd1,shell=True, stdout=output6, stderr=err6)
        output, error = process.communicate()
    elif op == "scal":
        for i in scale:
            lineitem = "/home/john/tpch-dbgen/data/lineitem.tbl." + str(i)
            output6 = open("output6o-" + str(i) + ".txt","w+")
            err6 = open("err6o-" + str(i) + ".txt","w+")
            cmd1 = "./bin/spark-submit --master spark://10.22.1.3:7081 --class edu.hku.dp.original.TPCH6 " + \
                   "--conf 'spark.executor.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
                   "--conf 'spark.driver.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
                   "--driver-memory 50g --executor-memory 50g --conf spark.executor.extraJavaOptions='-Xms50g' --conf spark.driver.extraJavaOptions='-Xms50g' " + \
                   "examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar " + \
                   lineitem
            process = subprocess.Popen(cmd1,shell=True, stdout=output6, stderr=err6)
            output, error = process.communicate()

if wq == "11o" or wq == "all":
    if op == "sing":
        partsupp = "/home/john/tpch-dbgen/data/partsupp.tbl." + str(sing)
        output11 = open("output11o-" + str(sing) + ".txt","w+")
        err11 = open("err11o-" + str(sing) + ".txt","w+")
        cmd1 = "./bin/spark-submit --master spark://10.22.1.3:7081 --class edu.hku.dp.original.TPCH11 " + \
               "--conf 'spark.executor.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
               "--conf 'spark.driver.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
               "--driver-memory 50g --executor-memory 50g --conf spark.executor.extraJavaOptions='-Xms50g' --conf spark.driver.extraJavaOptions='-Xms50g' " + \
               "examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar " + \
               supplier + " " + nation + " " + partsupp
        process = subprocess.Popen(cmd1,shell=True, stdout=output11, stderr=err11)
        output, error = process.communicate()
    elif op == "scal":
        for i in scale:
            partsupp = "/home/john/tpch-dbgen/data/partsupp.tbl." + str(i)
            output11 = open("output11o-" + str(i) + ".txt","w+")
            err11 = open("err11o-" + str(i) + ".txt","w+")
            cmd1 = "./bin/spark-submit --master spark://10.22.1.3:7081 --class edu.hku.dp.original.TPCH11 " + \
                   "--conf 'spark.executor.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
                   "--conf 'spark.driver.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
                   "--driver-memory 50g --executor-memory 50g --conf spark.executor.extraJavaOptions='-Xms50g' --conf spark.driver.extraJavaOptions='-Xms50g' " + \
                   "examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar " + \
                   supplier + " " + nation + " " + partsupp
            process = subprocess.Popen(cmd1,shell=True, stdout=output11, stderr=err11)
            output, error = process.communicate()

if wq == "13o" or wq == "all":
    if op == "sing":
        lineitem = "/home/john/tpch-dbgen/data/lineitem.tbl." + str(sing)
        output13 = open("output13o-" + str(sing) + ".txt","w+")
        err13 = open("err13o-" + str(sing) + ".txt","w+")
        cmd1 = "./bin/spark-submit --master spark://10.22.1.3:7081 --class edu.hku.dp.TPCH13 " + \
               "--conf 'spark.executor.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
               "--conf 'spark.driver.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
               "--driver-memory 50g --executor-memory 50g --conf spark.executor.extraJavaOptions='-Xms50g' --conf spark.driver.extraJavaOptions='-Xms50g' " + \
               "examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar " + \
               lineitem + " " + order
        process = subprocess.Popen(cmd1,shell=True, stdout=output13, stderr=err13)
        output, error = process.communicate()
    elif op == "scal":
        for i in scale:
            lineitem = "/home/john/tpch-dbgen/data/lineitem.tbl." + str(i)
            output13 = open("output13o-" + str(i) + ".txt","w+")
            err13 = open("err13o-" + str(i) + ".txt","w+")
            cmd1 = "./bin/spark-submit --master spark://10.22.1.3:7081 --class edu.hku.dp.TPCH13 " + \
                   "--conf 'spark.executor.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
                   "--conf 'spark.driver.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
                   "--driver-memory 50g --executor-memory 50g --conf spark.executor.extraJavaOptions='-Xms50g' --conf spark.driver.extraJavaOptions='-Xms50g' " + \
                   "examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar " + \
                   lineitem + " " + order
            process = subprocess.Popen(cmd1,shell=True, stdout=output13, stderr=err13)
            output, error = process.communicate()

if wq == "16o" or wq == "all":
    if op == "sing":
        partsupp = "/home/john/tpch-dbgen/data/partsupp.tbl." + str(sing)
        output16 = open("output16o-" + str(sing) + ".txt","w+")
        err16 = open("err16o-" + str(sing) + ".txt","w+")
        cmd1 = "./bin/spark-submit --master spark://10.22.1.3:7081 --class edu.hku.dp.TPCH16 " + \
               "--conf 'spark.executor.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
               "--conf 'spark.driver.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
               "--driver-memory 50g --executor-memory 50g --conf spark.executor.extraJavaOptions='-Xms50g' --conf spark.driver.extraJavaOptions='-Xms50g' " + \
               "examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar " + \
               part + " " + supplier + " " + partsupp
        process = subprocess.Popen(cmd1,shell=True, stdout=output16, stderr=err16)
        output, error = process.communicate()
    elif op == "scal":
        for i in scale:
            partsupp = "/home/john/tpch-dbgen/data/partsupp.tbl." + str(i)
            output16 = open("output16o-" + str(i) + ".txt","w+")
            err16 = open("err16o-" + str(i) + ".txt","w+")
            cmd1 = "./bin/spark-submit --master spark://10.22.1.3:7081 --class edu.hku.dp.TPCH16 " + \
                   "--conf 'spark.executor.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
                   "--conf 'spark.driver.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
                   "--driver-memory 50g --executor-memory 50g --conf spark.executor.extraJavaOptions='-Xms50g' --conf spark.driver.extraJavaOptions='-Xms50g' " + \
                   "examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar " + \
                   part + " " + supplier + " " + partsupp
            process = subprocess.Popen(cmd1,shell=True, stdout=output16, stderr=err16)
            output, error = process.communicate()

if wq == "21o" or wq == "all":
    if op == "sing":
        lineitem = "/home/john/tpch-dbgen/data/lineitem.tbl." + str(sing)
        output21 = open("output21o-" + str(sing) + ".txt","w+")
        err21 = open("err21o-" + str(sing) + ".txt","w+")
        cmd1 = "./bin/spark-submit --master spark://10.22.1.3:7081 --class edu.hku.dp.TPCH21 " + \
               "--conf 'spark.executor.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
               "--conf 'spark.driver.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
               "--driver-memory 50g --executor-memory 50g --conf spark.executor.extraJavaOptions='-Xms50g' --conf spark.driver.extraJavaOptions='-Xms50g' " + \
               "examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar " + \
               " " + supplier + " " + lineitem + " " + "/home/john/tpch-dbgen/data/lineitem.tbl.original" + \
               " " + order + " " + nation + " " + db
        process = subprocess.Popen(cmd1,shell=True, stdout=output21, stderr=err21)
        output, error = process.communicate()
    elif op == "scal":
        for i in scale:
            lineitem = "/home/john/tpch-dbgen/data/lineitem.tbl." + str(i)
            output21 = open("output21o-" + str(i) + ".txt","w+")
            err21 = open("err21o-" + str(i) + ".txt","w+")
            cmd1 = "./bin/spark-submit --master spark://10.22.1.3:7081 --class edu.hku.dp.TPCH21 " + \
                   "--conf 'spark.executor.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
                   "--conf 'spark.driver.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
                   "--driver-memory 50g --executor-memory 50g --conf spark.executor.extraJavaOptions='-Xms50g' --conf spark.driver.extraJavaOptions='-Xms50g' " + \
                   "examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar " + \
                   " " + supplier + " " + lineitem + " " + "/home/john/tpch-dbgen/data/lineitem.tbl.original" + \
                   " " + order + " " + nation + " " + db
            process = subprocess.Popen(cmd1,shell=True, stdout=output21, stderr=err21)
            output, error = process.communicate()

if wq == "lro" or wq == "all":
    if op == "sing":
        ml_data = "/home/john/tpch-dbgen/data/ml." + str(sing)
        output21 = open("outputlro-" + str(sing) + "," + str(sampleSize) + ".txt","w+")
        err21 = open("errlro-" + str(sing) + "," + str(sampleSize) + ".txt","w+")
        cmd1 = "./bin/spark-submit --master spark://10.22.1.3:7081 --class edu.hku.dp.original.SparkHdfsLR " + \
               "--conf 'spark.executor.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
               "--conf 'spark.driver.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
               "--driver-memory 50g --executor-memory 50g --conf spark.executor.extraJavaOptions='-Xms50g' --conf spark.driver.extraJavaOptions='-Xms50g' " + \
               "examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar " + \
               ml_data + " 1 9 " + \
               str(threshold) + " " + str(sampleSize)
        #./bin/spark-submit --master spark://10.22.1.3:7081 --class edu.hku.dp.TPCH21DP --conf "spark.executor.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps" --conf "spark.driver.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps" examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar
        process = subprocess.Popen(cmd1,shell=True, stdout=output21, stderr=err21)
        output, error = process.communicate()
    elif op == "scal":
        for i in scale:
            ml_data = "/home/john/tpch-dbgen/data/ml." + str(i)
            output21 = open("outputlro-" + str(i) + "," + str(sampleSize) + ".txt","w+")
            err21 = open("errlro-" + str(i) + "," + str(sampleSize) + ".txt","w+")
            cmd1 = "./bin/spark-submit --master spark://10.22.1.3:7081 --class edu.hku.dp.original.SparkHdfsLR " + \
                   "--conf 'spark.executor.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
                   "--conf 'spark.driver.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
                   "--driver-memory 50g --executor-memory 50g --conf spark.executor.extraJavaOptions='-Xms50g' --conf spark.driver.extraJavaOptions='-Xms50g' " + \
                   "examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar " + \
                   ml_data + " 1 9 " + \
                   str(threshold) + " " + str(sampleSize)
            process = subprocess.Popen(cmd1,shell=True, stdout=output21, stderr=err21)
            output, error = process.communicate()

if wq == "kmo" or wq == "all":
    if op == "sing":
        ml_data = "/home/john/tpch-dbgen/data/ml." + str(sing)
        output21 = open("outputkmo-" + str(sing) + "," + str(sampleSize) + ".txt","w+")
        err21 = open("errkmo-" + str(sing) + "," + str(sampleSize) + ".txt","w+")
        cmd1 = "./bin/spark-submit --master spark://10.22.1.3:7081 --class edu.hku.dp.original.SparkKMeans " + \
               "--conf 'spark.executor.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
               "--conf 'spark.driver.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
               "--driver-memory 50g --executor-memory 50g --conf spark.executor.extraJavaOptions='-Xms50g' --conf spark.driver.extraJavaOptions='-Xms50g' " + \
               "examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar " + \
               ml_data + " 1 3 10 " + \
               str(threshold) + " " + str(sampleSize)
        #./bin/spark-submit --master spark://10.22.1.3:7081 --class edu.hku.dp.TPCH21DP --conf "spark.executor.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps" --conf "spark.driver.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps" examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar
        process = subprocess.Popen(cmd1,shell=True, stdout=output21, stderr=err21)
        output, error = process.communicate()
    elif op == "scal":
        for i in scale:
            ml_data = "/home/john/tpch-dbgen/data/ml." + str(i)
            output21 = open("outputkmo-" + str(i) + "," + str(sampleSize) + ".txt","w+")
            err21 = open("errkmo-" + str(i) + "," + str(sampleSize) + ".txt","w+")
            cmd1 = "./bin/spark-submit --master spark://10.22.1.3:7081 --class edu.hku.dp.original.SparkKMeans " + \
                   "--conf 'spark.executor.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
                   "--conf 'spark.driver.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
                   "--driver-memory 50g --executor-memory 50g --conf spark.executor.extraJavaOptions='-Xms50g' --conf spark.driver.extraJavaOptions='-Xms50g' " + \
                   "examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar " + \
                   ml_data + " 1 3 10 " + \
                   str(threshold) + " " + str(sampleSize)
            process = subprocess.Popen(cmd1,shell=True, stdout=output21, stderr=err21)
            output, error = process.communicate()

for sampleSize in sp:
    if wq == "1c" or wq == "allc":
        if op == "sing":
            lineitem = "/home/john/tpch-dbgen/data/lineitem.tbl." + str(sing)
            output1 = open("output1c-" + str(sing) + "," + str(sampleSize) + ".txt","w+")
            err1 = open("err1c-" + str(sing) + "," + str(sampleSize) + ".txt","w+")
            cmd1 = "./bin/spark-submit --master spark://10.22.1.3:7081 --class edu.hku.dp.checker.TPCH1DP_checker " + \
                   "--conf 'spark.executor.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
                   "--conf 'spark.driver.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
                   "--driver-memory 50g --executor-memory 50g --conf spark.executor.extraJavaOptions='-Xms50g' --conf spark.driver.extraJavaOptions='-Xms50g' " + \
                   "examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar " + \
                   lineitem + " " + lineitem + "  "+ str(threshold) + " " + str(sampleSize)
            process = subprocess.Popen(cmd1,shell=True, stdout=output1, stderr=err1)
            output, error = process.communicate()
        elif op == "scal":
            for i in scale_c:
                lineitem = "/home/john/tpch-dbgen/data/lineitem.tbl." + str(i)
                output1 = open("output1c-" + str(i) + "," + str(sampleSize) + ".txt","w+")
                err1 = open("err1c-" + str(i) + "," + str(sampleSize) + ".txt","w+")
                cmd1 = "./bin/spark-submit --master spark://10.22.1.3:7081 --class edu.hku.dp.checker.TPCH1DP_checker " + \
                       "--conf 'spark.executor.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
                       "--conf 'spark.driver.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
                       "--driver-memory 50g --executor-memory 50g --conf spark.executor.extraJavaOptions='-Xms50g' --conf spark.driver.extraJavaOptions='-Xms50g' " + \
                       "examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar " + \
                       lineitem + " " + lineitem + "  "+ str(threshold) + " " + str(sampleSize)
                process = subprocess.Popen(cmd1,shell=True, stdout=output1, stderr=err1)
                output, error = process.communicate()

    if wq == "4c" or wq == "allc":
        if op == "sing":
            lineitem = "/home/john/tpch-dbgen/data/lineitem.tbl." + str(sing)
            output4 = open("output4c-" + str(sing) + "," + str(sampleSize) + ".txt","w+")
            err4 = open("err4c-" + str(sing) + "," + str(sampleSize) + ".txt","w+")
            cmd1 = "./bin/spark-submit --master spark://10.22.1.3:7081 --class edu.hku.dp.checker.TPCH4DP_checker " + \
                   "--conf 'spark.executor.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
                   "--conf 'spark.driver.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
                   "--driver-memory 50g --executor-memory 50g --conf spark.executor.extraJavaOptions='-Xms50g' --conf spark.driver.extraJavaOptions='-Xms50g' " + \
                   "examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar " + \
                   order + " " + order + "  " + lineitem + " " + lineitem + " " + str(threshold) + " " + str(sampleSize)
            process = subprocess.Popen(cmd1,shell=True, stdout=output4, stderr=err4)
            output, error = process.communicate()
        elif op == "scal":
            for i in scale_c:
                lineitem = "/home/john/tpch-dbgen/data/lineitem.tbl." + str(i)
                output4 = open("output4c-" + str(i) + "," + str(sampleSize) + ".txt","w+")
                err4 = open("err4c-" + str(i) + "," + str(sampleSize) + ".txt","w+")
                cmd1 = "./bin/spark-submit --master spark://10.22.1.3:7081 --class edu.hku.dp.checker.TPCH4DP_checker " + \
                       "--conf 'spark.executor.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
                       "--conf 'spark.driver.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
                       "--driver-memory 50g --executor-memory 50g --conf spark.executor.extraJavaOptions='-Xms50g' --conf spark.driver.extraJavaOptions='-Xms50g' " + \
                       "examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar " + \
                       order + " " + order + "  " + lineitem + " " + lineitem + " " + str(threshold) + " " + str(sampleSize)
                process = subprocess.Popen(cmd1,shell=True, stdout=output4, stderr=err4)
                output, error = process.communicate()

    if wq == "6c" or wq == "allc":
        if op == "sing":
            lineitem = "/home/john/tpch-dbgen/data/lineitem.tbl." + str(sing)
            output6 = open("output6c-" + str(sing) + "," + str(sampleSize) + ".txt","w+")
            err6 = open("err6c-" + str(sing) + "," + str(sampleSize) + ".txt","w+")
            cmd1 = "./bin/spark-submit --master spark://10.22.1.3:7081 --class edu.hku.dp.checker.TPCH6DP_checker " + \
                   "--conf 'spark.executor.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
                   "--conf 'spark.driver.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
                   "--driver-memory 50g --executor-memory 50g --conf spark.executor.extraJavaOptions='-Xms50g' --conf spark.driver.extraJavaOptions='-Xms50g' " + \
                   "examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar " + \
                   lineitem + " " + lineitem + " " + str(threshold) + " " + str(sampleSize)
            process = subprocess.Popen(cmd1,shell=True, stdout=output6, stderr=err6)
            output, error = process.communicate()
        elif op == "scal":
            for i in scale_c:
                lineitem = "/home/john/tpch-dbgen/data/lineitem.tbl." + str(i)
                output6 = open("output6c-" + str(i) + "," + str(sampleSize) + ".txt","w+")
                err6 = open("err6c-" + str(i) + "," + str(sampleSize) + ".txt","w+")
                cmd1 = "./bin/spark-submit --master spark://10.22.1.3:7081 --class edu.hku.dp.checker.TPCH6DP_checker " + \
                       "--conf 'spark.executor.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
                       "--conf 'spark.driver.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
                       "--driver-memory 50g --executor-memory 50g --conf spark.executor.extraJavaOptions='-Xms50g' --conf spark.driver.extraJavaOptions='-Xms50g' " + \
                       "examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar " + \
                       lineitem + " " + lineitem + " " + str(threshold) + " " + str(sampleSize)
                process = subprocess.Popen(cmd1,shell=True, stdout=output6, stderr=err6)
                output, error = process.communicate()

    if wq == "11c" or wq == "allc":
        if op == "sing":
            partsupp = "/home/john/tpch-dbgen/data/partsupp.tbl." + str(sing)
            output11 = open("output11c-" + str(sing) + "," + str(sampleSize) + ".txt","w+")
            err11 = open("err11c-" + str(sing) + "," + str(sampleSize) + ".txt","w+")
            cmd1 = "./bin/spark-submit --master spark://10.22.1.3:7081 --class edu.hku.dp.checker.TPCH11DP_checker " + \
                   "--conf 'spark.executor.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
                   "--conf 'spark.driver.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
                   "--driver-memory 50g --executor-memory 50g --conf spark.executor.extraJavaOptions='-Xms50g' --conf spark.driver.extraJavaOptions='-Xms50g' " + \
                   "examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar " + \
                   supplier + " " + supplier + " " + nation + " " + partsupp + \
                   " " + partsupp + " " + str(threshold) + " " + str(sampleSize)
            process = subprocess.Popen(cmd1,shell=True, stdout=output11, stderr=err11)
            output, error = process.communicate()
        elif op == "scal":
            for i in scale_c:
                partsupp = "/home/john/tpch-dbgen/data/partsupp.tbl." + str(i)
                output11 = open("output11c-" + str(i) + "," + str(sampleSize) + ".txt","w+")
                err11 = open("err11c-" + str(i) + "," + str(sampleSize) + ".txt","w+")
                cmd1 = "./bin/spark-submit --master spark://10.22.1.3:7081 --class edu.hku.dp.checker.TPCH11DP_checker " + \
                       "--conf 'spark.executor.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
                       "--conf 'spark.driver.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
                       "--driver-memory 50g --executor-memory 50g --conf spark.executor.extraJavaOptions='-Xms50g' --conf spark.driver.extraJavaOptions='-Xms50g' " + \
                       "examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar " + \
                       supplier + " " + supplier + " " + nation + " " + partsupp + \
                       " " + partsupp + " " + str(threshold) + " " + str(sampleSize)
                process = subprocess.Popen(cmd1,shell=True, stdout=output11, stderr=err11)
                output, error = process.communicate()

    if wq == "13c" or wq == "allc":
        if op == "sing":
            lineitem = "/home/john/tpch-dbgen/data/lineitem.tbl." + str(sing)
            output13 = open("output13c-" + str(sing) + "," + str(sampleSize) + ".txt","w+")
            err13 = open("err13c-" + str(sing) + "," + str(sampleSize) + ".txt","w+")
            cmd1 = "./bin/spark-submit --master spark://10.22.1.3:7081 --class edu.hku.dp.checker.TPCH13DP_checker " + \
                   "--conf 'spark.executor.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
                   "--conf 'spark.driver.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
                   "--driver-memory 50g --executor-memory 50g --conf spark.executor.extraJavaOptions='-Xms50g' --conf spark.driver.extraJavaOptions='-Xms50g' " + \
                   "examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar " + \
                   lineitem + " " + lineitem + " " + order + " " + order + \
                   " " + str(threshold) + " " + str(sampleSize)
            process = subprocess.Popen(cmd1,shell=True, stdout=output13, stderr=err13)
            output, error = process.communicate()
        elif op == "scal":
            for i in scale_c:
                lineitem = "/home/john/tpch-dbgen/data/lineitem.tbl." + str(i)
                output13 = open("output13c-" + str(i) + "," + str(sampleSize) + ".txt","w+")
                err13 = open("err13c-" + str(i) + "," + str(sampleSize) + ".txt","w+")
                cmd1 = "./bin/spark-submit --master spark://10.22.1.3:7081 --class edu.hku.dp.checker.TPCH13DP_checker " + \
                       "--conf 'spark.executor.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
                       "--conf 'spark.driver.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
                       "--driver-memory 50g --executor-memory 50g --conf spark.executor.extraJavaOptions='-Xms50g' --conf spark.driver.extraJavaOptions='-Xms50g' " + \
                       "examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar " + \
                       lineitem + " " + lineitem + " " + order + " " + order + \
                       " " + str(threshold) + " " + str(sampleSize)
                process = subprocess.Popen(cmd1,shell=True, stdout=output13, stderr=err13)
                output, error = process.communicate()

    if wq == "16c" or wq == "allc":
        if op == "sing":
            partsupp = "/home/john/tpch-dbgen/data/partsupp.tbl." + str(sing)
            output16 = open("output16c-" + str(sing) + "," + str(sampleSize) + ".txt","w+")
            err16 = open("err16c-" + str(sing) + "," + str(sampleSize) + ".txt","w+")
            cmd1 = "./bin/spark-submit --master spark://10.22.1.3:7081 --class edu.hku.dp.checker.TPCH16DP_checker " + \
                   "--conf 'spark.executor.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
                   "--conf 'spark.driver.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
                   "--driver-memory 50g --executor-memory 50g --conf spark.executor.extraJavaOptions='-Xms50g' --conf spark.driver.extraJavaOptions='-Xms50g' " + \
                   "examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar " + \
                   part + " " + part + " " + supplier + " " + supplier + \
                   " " + partsupp + " " + partsupp + " " + \
                   str(threshold) + " " + str(sampleSize)
            process = subprocess.Popen(cmd1,shell=True, stdout=output16, stderr=err16)
            output, error = process.communicate()
        elif op == "scal":
            for i in scale_c:
                partsupp = "/home/john/tpch-dbgen/data/partsupp.tbl." + str(i)
                output16 = open("output16c-" + str(i) + "," + str(sampleSize) + ".txt","w+")
                err16 = open("err16c-" + str(i) + "," + str(sampleSize) + ".txt","w+")
                cmd1 = "./bin/spark-submit --master spark://10.22.1.3:7081 --class edu.hku.dp.checker.TPCH16DP_checker " + \
                       "--conf 'spark.executor.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
                       "--conf 'spark.driver.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
                       "--driver-memory 50g --executor-memory 50g --conf spark.executor.extraJavaOptions='-Xms50g' --conf spark.driver.extraJavaOptions='-Xms50g' " + \
                       "examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar " + \
                       part + " " + part + " " + supplier + " " + supplier + \
                       " " + partsupp + " " + partsupp + " " + \
                       str(threshold) + " " + str(sampleSize)
                process = subprocess.Popen(cmd1,shell=True, stdout=output16, stderr=err16)
                output, error = process.communicate()

    if wq == "21c" or wq == "allc":
        if op == "sing":
            lineitem = "/home/john/tpch-dbgen/data/lineitem.tbl." + str(sing)
            output21 = open("output21c-" + str(sing) + "," + str(sampleSize) + ".txt","w+")
            err21 = open("err21c-" + str(sing) + "," + str(sampleSize) + ".txt","w+")
            cmd1 = "./bin/spark-submit --master spark://10.22.1.3:7081 --class edu.hku.dp.checker.TPCH21DP_checker " + \
                   "--conf 'spark.executor.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
                   "--conf 'spark.driver.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
                   "--driver-memory 50g --executor-memory 50g --conf spark.executor.extraJavaOptions='-Xms50g' --conf spark.driver.extraJavaOptions='-Xms50g' " + \
                   "examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar " + \
                   supplier + " " + supplier + " " + lineitem + " " + "/home/john/tpch-dbgen/data/lineitem.tbl.original" + \
                   " " + order + " " + order + " " + nation + " " + \
                   str(threshold) + " " + str(sampleSize) + " " + db
            #./bin/spark-submit --master spark://10.22.1.3:7081 --class edu.hku.dp.TPCH21DP --conf "spark.executor.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps" --conf "spark.driver.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps" examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar
            process = subprocess.Popen(cmd1,shell=True, stdout=output21, stderr=err21)
            output, error = process.communicate()
        elif op == "scal":
            for i in scale_c:
                lineitem = "/home/john/tpch-dbgen/data/lineitem.tbl." + str(i)
                output21 = open("output21c-" + str(i) + "," + str(sampleSize) + ".txt","w+")
                err21 = open("err21c-" + str(i) + "," + str(sampleSize) + ".txt","w+")
                cmd1 = "./bin/spark-submit --master spark://10.22.1.3:7081 --class edu.hku.dp.checker.TPCH21DP_checker " + \
                       "--conf 'spark.executor.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
                       "--conf 'spark.driver.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
                       "--driver-memory 50g --executor-memory 50g --conf spark.executor.extraJavaOptions='-Xms50g' --conf spark.driver.extraJavaOptions='-Xms50g' " + \
                       "examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar " + \
                       supplier + " " + supplier + " " + lineitem + " " + "/home/john/tpch-dbgen/data/lineitem.tbl.original" + \
                       " " + order + " " + order + " " + nation + " " + \
                       str(threshold) + " " + str(sampleSize) + " " + db
                process = subprocess.Popen(cmd1,shell=True, stdout=output21, stderr=err21)
                output, error = process.communicate()

    if wq == "lrc" or wq == "all" or wq == "upa":
        if op == "sing":
            ml_data = "/home/john/tpch-dbgen/data/ml." + str(sing)
            output21 = open("outputlrc-" + str(sing) + "," + str(sampleSize) + ".txt","w+")
            err21 = open("errlrc-" + str(sing) + "," + str(sampleSize) + ".txt","w+")
            cmd1 = "./bin/spark-submit --master spark://10.22.1.3:7081 --class edu.hku.dp.checker.SparkHdfsLRDP_checker " + \
                   "--conf 'spark.executor.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
                   "--conf 'spark.driver.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
                   "--driver-memory 50g --executor-memory 50g --conf spark.executor.extraJavaOptions='-Xms50g' --conf spark.driver.extraJavaOptions='-Xms50g' " + \
                   "examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar " + \
                   ml_data + " 1 9 " + \
                   str(threshold) + " " + str(sampleSize)
            #./bin/spark-submit --master spark://10.22.1.3:7081 --class edu.hku.dp.TPCH21DP --conf "spark.executor.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps" --conf "spark.driver.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps" examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar
            process = subprocess.Popen(cmd1,shell=True, stdout=output21, stderr=err21)
            output, error = process.communicate()
        elif op == "scal":
            for i in scale:
                ml_data = "/home/john/tpch-dbgen/data/ml." + str(i)
                output21 = open("outputlrc-" + str(i) + "," + str(sampleSize) + ".txt","w+")
                err21 = open("errlrc-" + str(i) + "," + str(sampleSize) + ".txt","w+")
                cmd1 = "./bin/spark-submit --master spark://10.22.1.3:7081 --class edu.hku.dp.checker.SparkHdfsLRDP_checker " + \
                       "--conf 'spark.executor.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
                       "--conf 'spark.driver.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
                       "--driver-memory 50g --executor-memory 50g --conf spark.executor.extraJavaOptions='-Xms50g' --conf spark.driver.extraJavaOptions='-Xms50g' " + \
                       "examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar " + \
                       ml_data + " 1 9 " + \
                       str(threshold) + " " + str(sampleSize)
                process = subprocess.Popen(cmd1,shell=True, stdout=output21, stderr=err21)
                output, error = process.communicate()

    if wq == "kmc" or wq == "all" or wq == "upa":
        if op == "sing":
            ml_data = "/home/john/tpch-dbgen/data/ml." + str(sing)
            output21 = open("outputkmc-" + str(sing) + "," + str(sampleSize) + ".txt","w+")
            err21 = open("errkmc-" + str(sing) + "," + str(sampleSize) + ".txt","w+")
            cmd1 = "./bin/spark-submit --master spark://10.22.1.3:7081 --class edu.hku.dp.checker.SparkKMeansDP_checker " + \
                   "--conf 'spark.executor.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
                   "--conf 'spark.driver.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
                   "--driver-memory 50g --executor-memory 50g --conf spark.executor.extraJavaOptions='-Xms50g' --conf spark.driver.extraJavaOptions='-Xms50g' " + \
                   "examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar " + \
                   ml_data + " 1 3 10 " + \
                   str(threshold) + " " + str(sampleSize)
            #./bin/spark-submit --master spark://10.22.1.3:7081 --class edu.hku.dp.TPCH21DP --conf "spark.executor.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps" --conf "spark.driver.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps" examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar
            process = subprocess.Popen(cmd1,shell=True, stdout=output21, stderr=err21)
            output, error = process.communicate()
        elif op == "scal":
            for i in scale:
                ml_data = "/home/john/tpch-dbgen/data/ml." + str(i)
                output21 = open("outputkmc-" + str(i) + "," + str(sampleSize) + ".txt","w+")
                err21 = open("errkmc-" + str(i) + "," + str(sampleSize) + ".txt","w+")
                cmd1 = "./bin/spark-submit --master spark://10.22.1.3:7081 --class edu.hku.dp.checker.SparkKMeansDP_checker " + \
                       "--conf 'spark.executor.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
                       "--conf 'spark.driver.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' " + \
                       "--driver-memory 50g --executor-memory 50g --conf spark.executor.extraJavaOptions='-Xms50g' --conf spark.driver.extraJavaOptions='-Xms50g' " + \
                       "examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar " + \
                       ml_data + " 1 3 10 " + \
                       str(threshold) + " " + str(sampleSize)
                process = subprocess.Popen(cmd1,shell=True, stdout=output21, stderr=err21)
                output, error = process.communicate()