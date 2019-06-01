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
