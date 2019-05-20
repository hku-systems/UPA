#!/bin/bash
echo "Start" > output.txt
#opaque1

formateddate=`date | sed "s~ ~-~g" | sed "s~:~-~g"`

for i in {1..2000}
do

./bin/spark-submit --master spark://10.22.1.3:7081 --driver-memory 60g  --executor-memory 60g --conf spark.executor.extraJavaOptions="-Xms60g" --conf spark.driver.maxResultSize="0" --class main.scala.Q31 examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar $i >> outputs/Q31/output$formateddate.txt

./bin/spark-submit --master spark://10.22.1.3:7081 --driver-memory 60g  --executor-memory 60g --conf spark.executor.extraJavaOptions="-Xms60g" --conf spark.driver.maxResultSize="0" --class main.scala.Q34 examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar $i >> outputs/Q34/output$formateddate.txt

./bin/spark-submit --master spark://10.22.1.3:7081 --driver-memory 60g  --executor-memory 60g --conf spark.executor.extraJavaOptions="-Xms60g" --conf spark.driver.maxResultSize="0" --class main.scala.Q43 examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar $i >> outputs/Q43/output$formateddate.txt

./bin/spark-submit --master spark://10.22.1.3:7081 --driver-memory 60g  --executor-memory 60g --conf spark.executor.extraJavaOptions="-Xms60g" --conf spark.driver.maxResultSize="0" --class main.scala.Q46 examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar $i >> outputs/Q46/output$formateddate.txt

done