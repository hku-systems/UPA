# UPA

UPA is a big-data system that automatically infers a sensitivity value for enforcing Differential Privacy. 
Below shows a simple example demonstrating the functionalities of UPA.

## Core dependencies

`sudo apt-get insall openjdk-8-jdk maven`

## How to build UPA

UPA is built in the same way of Apache Spark i.e., by running:

`build/mvn -DskipTests -T 40 package`

## Running an example

1.Generate a sample dataset:

`mkdir $HOME/test; python gen_data.py --wq simple --path $HOME/test/dataset.txt --s 100000`

This will create a dataset of 100000 records for testing under `$HOME/test/dataset.txt`

2.Parition the dataset:

`python indexing.py --wq index --path $HOME/test/dataset.txt`

This will partition the dataset (`$HOME/test/dataset.txt`) into two partitions, 
the partitioned dataset is `$HOME/test/dataset.txt.upa` and to be read by UPA.

3.Running an example: 

`./demo_attack.sh`

The outputs are in `output.txt`. Detailed description can be found in the shell file

## Run UPA in cluster mode

First start a master by running the following command on a master computer:

`./sbin/start-master.sh -h <ip address of master> -p <port to be used>`

Then start workers by running the following command on a worker computer:

`./sbin/start-slave.sh spark://<ip address of master>:<port to be used>`

Then running `./demo_attack.sh` on the master computer. Note that the input dataset has to be replicated on both master and workers. 
After finishing testing, stop the master and workers by running `./sbin/stop-master.sh` and `./sbin/stop-slave.sh` on master and worker respectively, to release network resources.



