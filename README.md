# UPA

UPA is a big-data system that automatically infers a sensitivity value for enforcing Differential Privacy. 
Below shows a simple example demonstrating the functionalities of UPA.

## How to build UPA

UPA is built in the same way of Apache Spark i.e., by running:

`build/mvn -DskipTests -T 40 package`

## Running an example

1.Generate a sample dataset:

`mkdir $HOME/test; python gen_data.py --wq simple --path $HOME/test/dataset.txt --s 100000`

This will create a dataset for testing under `$HOME/test/dataset.txt`

2.Parition the dataset:

`python indexing.py --wq index --path $HOME/test/dataset.txt`

This will partition the dataset (`$HOME/test/dataset.txt`) into two partitions, 
the partitioned dataset is `$HOME/test/dataset.txt.upa` and to be read by UPA.

3.Running query an example query: 

`python tests.py --wq simple --path $HOME/test/dataset.txt.upa --attack yes`

By setting `--attack yes`, a differential attack is conducted by running the same query twice, 
whose input datasets differ by only one data record (a data record is randomly removed from the input 
dataset). UPA will detect and avoid the attack ("differential attack is detected and avoided" is printed), 
and outputs the noisy computation result.

By setting `--attack yes`, a differential attack is not conducted so UPA simply outputs 
the noisy computation result.

