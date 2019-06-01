#!/usr/bin/env bash
#The following command is for testing locally
#path_l="/home/john/tpch-spark/dbgen/lineitem.ftbl"; ./bin/spark-submit --class edu.hku.dp.TPCH1 examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar $path_l
#path_l="/home/john/tpch-spark/dbgen/lineitem.ftbl"; ./bin/spark-submit --class edu.hku.dp.TPCH6 examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar $path_l
#path_l="/home/john/tpch-spark/dbgen/lineitem.ftbl"; ./bin/spark-submit --class edu.hku.dp.TPCH4 examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar "/home/john/tpch-spark/dbgen/orders.ftbl" $path_l
#path_l="/home/john/tpch-spark/dbgen/lineitem.ftbl"; ./bin/spark-submit --class edu.hku.dp.TPCH13 examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar "/home/john/tpch-spark/dbgen/orders.ftbl" $path_l
#path_l="/home/john/tpch-spark/dbgen/lineitem.ftbl"; ./bin/spark-submit --class edu.hku.dp.TPCH21 examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar "/home/john/tpch-spark/dbgen/supplier.ftbl" $path_l "/home/john/tpch-spark/dbgen/orders.ftbl" "/home/john/tpch-spark/dbgen/nation.ftbl"
#
#path_p="/home/john/tpch-spark/dbgen/partsupp.ftbl"; ./bin/spark-submit --class edu.hku.dp.TPCH11 examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar "/home/john/tpch-spark/dbgen/supplier.tbl" "/home/john/tpch-spark/dbgen/nation.tbl" $path_p
#path_p="/home/john/tpch-spark/dbgen/partsupp.ftbl"; ./bin/spark-submit --class edu.hku.dp.TPCH16 examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar "/home/john/tpch-spark/dbgen/part.tbl" "/home/john/tpch-spark/dbgen/supplier.tbl" $path_p

./bin/spark-submit --master spark://10.22.1.6:7081 --driver-memory 30g --executor-memory 30g --conf spark.executor.extraJavaOptions="-Xms30g" --conf spark.driver.maxResultSize="0" --class edu.hku.dp.SparkKMeans examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar /home/john/tpch-spark/dataset/ds/ds1.10_a.csv 1 3 10 > outputOKM.txt 2> errOKM.txt
./bin/spark-submit --master spark://10.22.1.6:7081 --driver-memory 30g --executor-memory 30g --conf spark.executor.extraJavaOptions="-Xms30g" --conf spark.driver.maxResultSize="0" --class edu.hku.dp.SparkKMeansDP examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar /home/john/tpch-spark/dataset/ds/ds1.10_a.csv /home/john/tpch-spark/dataset/ds/ds1.10_a.csv 1 3 10 1 > outputKM.txt 2> errKM.txt