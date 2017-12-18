# node2vec #
Spark+scala implementation of neighborhood sampling and feature learning for large graphs:

This repository includes the implementation of node2vec (Grover, Aditya, and Jure Leskovec. "node2vec: Scalable feature learning for networks." Proceedings of the 22nd ACM SIGKDD international conference on Knowledge discovery and data mining. ACM, 2016.).

# Features #
* Scalable node2vec
* Second-order random walk
* Memory-efficient graph data structure introduced @ Spark Summit 2017 "Random Walks on Large Scale Graphs with Apache Spark" presented by Min Shen (LinkedIn)
* Leverages graph partition information in order to optimize the communication and to speed up the random walk computation
* Compatible with [Spark-JobServer](https://github.com/spark-jobserver/spark-jobserver)

# Requirements #

# Quick Setup #


