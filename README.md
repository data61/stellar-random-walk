# node2vec #
Spark+scala implementation of neighborhood sampling and feature learning for large graphs:

This repository includes the implementation of node2vec.

## Features ##
* Scalable node2vec
* Second-order random walk
* Memory-efficient graph data structure introduced @ Spark Summit 2017 "Random Walks on Large Scale Graphs with Apache Spark" presented by Min Shen (LinkedIn)
* Leverages graph partition information in order to optimize the communication and to speed up the random walk computation
* Compatible with [Spark-JobServer](https://github.com/spark-jobserver/spark-jobserver)

## Requirements ##
* Scala 2.11 or later.
* Maven 3+
* Java 8+
* Apache Spark 2.2.0 or later.
* (Optional): Spark-JobServer 0.8.0 or later.

## Quick Setup ##
First, you need to download the application source code. You need to go to the source code directory and run the following command:

` mvn clean package `

This creates a jar file named *randomwalk-0.0.1-SNAPSHOT.jar* in the directory *target*. To run the application, you use this jar file.

You need to download Apache Spark 2.2.0 or later in order to run the application.

## Running the Application ##
To run the application on your machine, you can use spark-submit script. Go to the Apache Spark directory. Run the application with the following command:

` bin/spark-submit --class au.csiro.data61.randomwalk.Main ./randomwalk/target/randomwalk-0.0.1-SNAPSHOT.jar `

and the following options are available:

```
--walkLength <value>     walkLength: 80
   --numWalks <value>       numWalks: 10
   --p <value>              return parameter p: 1.0
   --q <value>              in-out parameter q: 1.0
   --rddPartitions <value>  Number of RDD partitions in running Random Walk and Word2vec: 200
   --weighted <value>       weighted: true
   --directed <value>       directed: false
   --w2vPartitions <value>  Number of partitions in word2vec: 10
   --input <value>          Input edge file path: empty
   --output <value>         Output path: empty
   --cmd <value>            command: node2vec
   --kryo <value>           Whether to use kryo serializer or not: false
   --partitioned <value>    Whether the graph is partitioned: false
   --lr <value>             Learning rate in word2vec: 0.025
   --iter <value>           Number of iterations in word2vec: 10
   --dim <value>            Number of dimensions in word2vec: 128
   --window <value>         Window size in word2vec: 10
   ```

   For example:

   ```
   bin/spark-submit --class au.csiro.data61.randomwalk.Main ./randomwalk/target/randomwalk-0.0.1-SNAPSHOT.jar \
   --cmd randomwalk --numWalks 1 --p 1 --q 1 --walkLength 10 --rddPartitions 10 \
   --input [input edge list] --output [output directory] --partitioned false
   ```

## Graph File Format ##
The input graph must be an edge list with integer vertex IDs. For example:

``` 
src1-id dst1-id
src1-id dst2-id
... 
```

If the graph is weighted, it must include the weight in the last column for each edge. For example:

` src1-id dst1-id 1.0 `

If the graph is partitioned, each edge should have a partition number, i.e., should be assigned to a partition. The partition number must be in the third column of the edge list. For example:

``` 
src1-id dst1-id 1 1.0
src1-id dst2-id 1 1.0
src3-id dst1-id 2 1.0
... 
```

The application itself will replicate (cut) those vertices that span among multiple partitions.


## References ##
1. (Grover, Aditya, and Jure Leskovec. "node2vec: Scalable feature learning for networks." Proceedings of the 22nd ACM SIGKDD international conference on Knowledge discovery and data mining. ACM, 2016.).
2. Mikolov, Tomas, et al. "Efficient estimation of word representations in vector space." arXiv preprint arXiv:1301.3781 (2013).
3. [Random Walks on Large Scale Graphs with Apache Spark](https://spark-summit.org/2017/events/random-walks-on-large-scale-graphs-with-apache-spark/)









