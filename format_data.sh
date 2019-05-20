#!/bin/bash
cd ~/tpch-spark/dbgen
mkdir ground-truth
formateddate=`date | sed "s~ ~-~g" | sed "s~:~-~g"`

for i in {1..2000}
do

#rename all *.tbl to *_tmp.tbl
allfile=`ls *.tbl`
for file in $allfile
do
filename=`echo "$file" | sed -e "s,.tbl,,g"`
#echo "$filename"
#cp $file $filename.ftbl
cat $file | head -n 2000 | sed -e "$i d" > ground-truth/$filename$i.ftbl
#sed -i "1d;2000d" $filename.ftbl
##sed -i "$id" $filename.ftbl
#echo "`cat $filename.ftbl | wc -l `"
done

#cd ~/AutoDP
#
#./send_data.sh
#
#./bin/spark-submit --master spark://10.22.1.3:7081 --driver-memory 60g  --executor-memory 60g --conf spark.executor.extraJavaOptions="-Xms60g" --conf spark.driver.maxResultSize="0" --class main.scala.Q31 examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar 100 >> outputs/Q31/output$formateddate.txt
#
#./bin/spark-submit --master spark://10.22.1.3:7081 --driver-memory 60g  --executor-memory 60g --conf spark.executor.extraJavaOptions="-Xms60g" --conf spark.driver.maxResultSize="0" --class main.scala.Q34 examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar 100 >> outputs/Q34/output$formateddate.txt
#
#./bin/spark-submit --master spark://10.22.1.3:7081 --driver-memory 60g  --executor-memory 60g --conf spark.executor.extraJavaOptions="-Xms60g" --conf spark.driver.maxResultSize="0" --class main.scala.Q43 examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar 100 >> outputs/Q43/output$formateddate.txt
#
#./bin/spark-submit --master spark://10.22.1.3:7081 --driver-memory 60g  --executor-memory 60g --conf spark.executor.extraJavaOptions="-Xms60g" --conf spark.driver.maxResultSize="0" --class main.scala.Q46 examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar 100 >> outputs/Q46/output$formateddate.txt
#
#cd ~/tpch-spark/dbgen
#./home/john/AutoDP/bin/spark-submit --master spark://10.22.1.3:7081 --driver-memory 60g  --executor-memory 60g --conf spark.executor.extraJavaOptions="-Xms60g" --conf spark.driver.maxResultSize="0" --class main.scala.Q51 examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar 100 >> ~/AutoDP/outputs/Q51/output$formateddate.txt
done

