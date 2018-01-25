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
1. Get the random walk application source code:

    * using git: ' git clone git@github.com:data61/stellar-random-walk.git '
    * using http: download the [zip file](https://github.com/data61/stellar-random-walk/archive/master.zip) and unzip it.
    
2. Go to the source code directory. A pre-built jar file, named *randomwalk-0.0.1-SNAPSHOT.jar*, is available at *./target*. To run the application, you use this jar file. (If you want to build the jar file from the source code, you need to have Apache Maven installed and run: ` mvn clean package `)

3. Download [Apache Spark 2.2.0 or later](https://spark.apache.org/downloads.html) (e.g release 2.2.1, pre-built for apache hadoop 2.7)

### Run the Application Using Spark (local machine) ###
To run the application on your machine, you can use spark-submit script. Go to the Apache Spark directory. Run the application with the following command:

` bin/spark-submit --class au.csiro.data61.randomwalk.Main [random walk dir]/target/randomwalk-0.0.1-SNAPSHOT.jar `

### Run the Application Using Spark Job-server ###
4. make sure that the prerequisites are installed: 
    - [Apache spark](https://spark.apache.org/downloads.html) (e.g release 2.2.1, pre-built for apache hadoop 2.7) 
    - Java Virtual Machine (e.g. 9.0.1)
    - [sbt](https://www.scala-sbt.org/)

5. git clone [job-server](https://github.com/spark-jobserver/spark-jobserver)
6. Create a `.bashrc` with the paths to JVM, sbt and spark, e.g., for Mac OS users it will be the following:

    `
    export SBT=/usr/local/Cellar/sbt/1.1.0
    export SPARK_HOME=~/spark-2.2.1-bin-hadoop2.7
    export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk-9.0.1.jdk
    export PATH=$JAVA_HOME/bin:$SBT/bin:$PATH
    `

7. Run 

      `source .bashrc`
        
8. Go to spark folder and run `start-all.sh`:
```
cd spark-2.2.1-bin-hadoop2.7/
sbin/sbin/start-all.sh
```
9. From spark Job-server run sbt shell:
```
cd spark-jobserver/job-server/
sbt
```
10. Once inside sbt shell, run `reStart`.

11. Go to `http://localhost:8090/` and make sure that Spark Job Server UI is working (Note: in Chrome binaries were not updated properly, while in Firefox it was ok)

12. upload randomwalk jar to the server:

    `curl --data-binary @randomwalk/target/randomwalk-0.0.1-SNAPSHOT.jar localhost:8090/jars/randomwalk`

13. submit a job:

    `curl -d "rw.input = --cmd randomwalk --numWalks 1 --p 1 --q 1 --walkLength 10 --rddPartitions 10 --directed false --input [random walk dir]/src/test/resources/karate.txt --output [output dir] --partitioned false" 'localhost:8090/jobs?appName=randomwalk&classPath=au.csiro.data61.randomwalk.Main'`

14. Check the status in the Spark Job-server UI


## Application Options ##
The following options are available:

```
   --walkLength <value>     walkLength: 80
   --numWalks <value>       numWalks: 10
   --p <value>              return parameter p: 1.0
   --q <value>              in-out parameter q: 1.0
   --rddPartitions <value>  Number of RDD partitions in running Random Walk and Word2vec: 200
   --weighted <value>       weighted: true
   --directed <value>       directed: false
   --w2vPartitions <value>  Number of partitions in word2vec: 10
   --input <value>          Input edge-file/paths-file: empty
   --output <value>         Output path: empty
   --cmd <value>            command: randomwalk/embedding/node2vec (to run randomwalk + embedding)
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


You can choose the algorithm to run by the --cmd option:
- **randomwalk**: to run just the second-order randomwalk.
- **embedding**: to run just word2vec given paths as input.
- **node2vec**: to run randomwalk + embedding
     
## Graph Input File Format ##
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

## Output Format ##
The application writes output to the disk in the directory given by the --output parameter. In this directory, three folders may be created:
   - /path: the result for randomwalk.
   - /vec: vector representation of vertices.
   - /bin: word2vec model's metadata.
   
The output of the randomwalk (in the /path directory) is in the format of tab-separated integers (vertex-ids), where each line represents random steps starting from a vertex. The number of lines are equal to the number of generated paths (--numWalks*|V|). The result is partitioned in --rddPartitions number of files as plain text.

The embeddings are also in the the format of tab-separated numbers per line, where the first number represents the vertex-id and the rest of the numbers in that line represent the vertex's vector representation. The result is partitioned in --rddPartition number of files and is written as plain text.


## References ##
1. (Grover, Aditya, and Jure Leskovec. "node2vec: Scalable feature learning for networks." Proceedings of the 22nd ACM SIGKDD international conference on Knowledge discovery and data mining. ACM, 2016.).
2. Mikolov, Tomas, et al. "Efficient estimation of word representations in vector space." arXiv preprint arXiv:1301.3781 (2013).
3. [Random Walks on Large Scale Graphs with Apache Spark](https://spark-summit.org/2017/events/random-walks-on-large-scale-graphs-with-apache-spark/)









