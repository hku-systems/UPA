#!/usr/bin/env bash

#**********Linitems**************
#Only change lineitem i.e., change path_l in the following commands
#Ours
path_l="/home/john/tpch-spark/dbgen/lineitem.tbl" #Change this path to test different size of dataset
./bin/spark-submit --master spark://10.22.1.3:7081 --driver-memory 30g --executor-memory 30g --conf spark.executor.extraJavaOptions="-Xms30g" --class edu.hku.dp.TPCH1DP examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar $path_l "/home/john/tpch-spark/dbgen/lineitem.tbl" 1 > output1.txt 2> err1.txt
./bin/spark-submit --master spark://10.22.1.3:7081 --driver-memory 30g --executor-memory 30g --conf spark.executor.extraJavaOptions="-Xms30g" --class edu.hku.dp.TPCH6DP examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar $path_l "/home/john/tpch-spark/dbgen/lineitem.tbl" 1 1>output6.txt 2> err6.txt
./bin/spark-submit --master spark://10.22.1.3:7081 --driver-memory 30g --executor-memory 30g --conf spark.executor.extraJavaOptions="-Xms30g" --class edu.hku.dp.TPCH4DP examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar "/home/john/tpch-spark/dbgen/orders.tbl" "/home/john/tpch-spark/dbgen/orders.tbl" $path_l "/home/john/tpch-spark/dbgen/lineitem.tbl" 1 1> output4.txt 2> err4.txt
./bin/spark-submit --master spark://10.22.1.3:7081 --driver-memory 30g --executor-memory 30g --conf spark.executor.extraJavaOptions="-Xms30g" --class edu.hku.dp.TPCH13DP examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar "/home/john/tpch-spark/dbgen/orders.tbl" "/home/john/tpch-spark/dbgen/orders.tbl" $path_l "/home/john/tpch-spark/dbgen/lineitem.tbl" 1 1> output13.txt 2>err13.txt
./bin/spark-submit --master spark://10.22.1.3:7081 --driver-memory 30g --executor-memory 30g --conf spark.executor.extraJavaOptions="-Xms30g" --class edu.hku.dp.TPCH21DP examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar "/home/john/tpch-spark/dbgen/supplier.tbl" "/home/john/tpch-spark/dbgen/supplier.tbl" $path_l "/home/john/tpch-spark/dbgen/lineitem.tbl" "/home/john/tpch-spark/dbgen/orders.tbl" "/home/john/tpch-spark/dbgen/orders.tbl" "/home/john/tpch-spark/dbgen/nation.tbl" 1 1> output21.txt 2>err21.txt

#Original
./bin/spark-submit --master spark://10.22.1.3:7081 --driver-memory 30g --executor-memory 30g --conf spark.executor.extraJavaOptions="-Xms30g" --class edu.hku.dp.TPCH1 examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar $path_l 1 > outputO1.txt 2> errO1.txt
./bin/spark-submit --master spark://10.22.1.3:7081 --driver-memory 30g --executor-memory 30g --conf spark.executor.extraJavaOptions="-Xms30g" --class edu.hku.dp.TPCH6 examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar $path_l 1>outputO6.txt 2> errO6.txt
./bin/spark-submit --master spark://10.22.1.3:7081 --driver-memory 30g --executor-memory 30g --conf spark.executor.extraJavaOptions="-Xms30g" --class edu.hku.dp.TPCH4 examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar "/home/john/tpch-spark/dbgen/orders.tbl" $path_l 1> outputO4.txt 2> errO4.txt
./bin/spark-submit --master spark://10.22.1.3:7081 --driver-memory 30g --executor-memory 30g --conf spark.executor.extraJavaOptions="-Xms30g" --class edu.hku.dp.TPCH13 examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar "/home/john/tpch-spark/dbgen/orders.tbl" $path_l 1 1> outputO13.txt 2>errO13.txt
./bin/spark-submit --master spark://10.22.1.3:7081 --driver-memory 30g --executor-memory 30g --conf spark.executor.extraJavaOptions="-Xms30g" --class edu.hku.dp.TPCH21 examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar "/home/john/tpch-spark/dbgen/supplier.tbl" $path_l "/home/john/tpch-spark/dbgen/orders.tbl" "/home/john/tpch-spark/dbgen/nation.tbl" 1> outputO21.txt 2>errO21.txt


#**********partsupp**************
#Only change partsupp i.e., change *partsupp* in the following commands
#Ours
path_p="/home/john/tpch-spark/dbgen/large_data/partsupp*" #Change this path to test different size of dataset
./bin/spark-submit --master spark://10.22.1.3:7081 --driver-memory 30g --executor-memory 30g --conf spark.executor.extraJavaOptions="-Xms30g" --class edu.hku.dp.TPCH11DP examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar "/home/john/tpch-spark/dbgen/supplier.tbl" "/home/john/tpch-spark/dbgen/supplier.tbl" "/home/john/tpch-spark/dbgen/nation.tbl" $path_p "/home/john/tpch-spark/dbgen/partsupp.tbl" 1 > output11.txt 2> err11.txt
./bin/spark-submit --master spark://10.22.1.3:7081 --driver-memory 30g --executor-memory 30g --conf spark.executor.extraJavaOptions="-Xms30g" --class edu.hku.dp.TPCH16DP examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar "/home/john/tpch-spark/dbgen/part.tbl" "/home/john/tpch-spark/dbgen/part.tbl" "/home/john/tpch-spark/dbgen/supplier.tbl" "/home/john/tpch-spark/dbgen/supplier.tbl" $path_p "/home/john/tpch-spark/dbgen/partsupp.tbl" 1 > output16.txt 2> err16.txt

#Original
./bin/spark-submit --master spark://10.22.1.3:7081 --driver-memory 30g --executor-memory 30g --conf spark.executor.extraJavaOptions="-Xms30g" --class edu.hku.dp.TPCH11 examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar "/home/john/tpch-spark/dbgen/supplier.tbl" "/home/john/tpch-spark/dbgen/nation.tbl" $path_p > outputO11.txt 2> errO11.txt
./bin/spark-submit --master spark://10.22.1.3:7081 --driver-memory 30g --executor-memory 30g --conf spark.executor.extraJavaOptions="-Xms30g" --class edu.hku.dp.TPCH16 examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar "/home/john/tpch-spark/dbgen/part.tbl" "/home/john/tpch-spark/dbgen/supplier.tbl" $path_p > outputO16.txt 2> errO16.txt
