#!/bin/bash

#mkdir $HOME/test; python gen_data.py --wq simple --path $HOME/test/dataset.txt --s 100000

#python indexing.py --wq index --path $HOME/test/dataset.txt

rm histoutputs.csv;touch histoutputs.csv

./bin/spark-submit --class edu.hku.dp.e2e.sum_all \
examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar \
$HOME/test/dataset.txt.upa 1111 0 > output.txt

./bin/spark-submit --class edu.hku.dp.e2e.count_record \
examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar \
$HOME/test/dataset.txt.upa 1111 3 >> output.txt

./bin/spark-submit --class edu.hku.dp.e2e.extract \
examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar \
$HOME/test/dataset.txt.upa 1111 0 >> output.txt

./bin/spark-submit --class edu.hku.dp.e2e.count_record \
examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar \
$HOME/test/dataset.txt.upa 1111 4 >> output.txt
#by setting the last parameter as 1, one item from the input dataset
#is removed. This is a differential attack
#because this query has been submitted before while this time only
# one item is removed from the input dataset,
# as a result, "differential attack is
# detected and avoided"

./bin/spark-submit --class edu.hku.dp.e2e.count_record \
examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar \
$HOME/test/dataset.txt.upa 1111 100 >> output.txt
#by setting the last parameter as 100, 100 items from the input dataset
#are removed. This is not a differential attack so "differential attack is
# detected and avoided" may not be displayed (there are non-zero probability
# that the messsage is still display because UPA has zero false-negative rate
# in detecting such attack, but non-zero false positive rate).
