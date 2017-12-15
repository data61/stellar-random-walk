# README #
Spark implementation of neighborhood sampling for feature learning in large graphs such as: node2vec and metapath2vec

This implementation benefits from:
- an efficient in-memory graph data structure introduced @ Spark Summit 2017 "Random Walks on Large Scale Graphs with Apache Spark" presented by Min Shen (LinkedIn)
- graph partition information to optimize the communication and speed up the random walk computation
