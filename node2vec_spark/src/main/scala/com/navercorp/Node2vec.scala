package com.navercorp

import java.io.Serializable
import scala.util.Try
import org.slf4j.{LoggerFactory, Logger}
import org.apache.spark.{SparkContext, HashPartitioner}
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.{Graph, _}
import org.apache.spark.storage.StorageLevel
import com.navercorp.graph.{GraphOps, EdgeAttr, NodeAttr}
import com.navercorp.common.Property

object Node2vec extends Serializable {
  lazy val logger: Logger = LoggerFactory.getLogger(getClass.getName)

  var context: SparkContext = _
  var config: Main.Params = _
  var label2id: RDD[(String, Long)] = _ // a label per node id?

  def setup(context: SparkContext, param: Main.Params): this.type = {
    this.context = context
    this.config = param

    this
  }

  /**
    * Loads the graph and computes the probabilities to go from each vertex to its neighbors
    *
    * @return
    */
  def loadGraph() = {
    // the directed and weighted parameters are only used for building the graph object.
    // is directed? they will be shared among stages and executors
    val bcDirected = context.broadcast(config.directed)
    val bcWeighted = context.broadcast(config.weighted) // is weighted?
    // inputTriplets is an array of edges (src, dst, weight).
    val inputTriplets = context.textFile(config.input).flatMap { triplet =>
      val parts = triplet.split("\\s")
      // if the weights are not specified it sets it to 1.0
      val weight = bcWeighted.value match {
        case true => Try(parts.last.toDouble).getOrElse(1.0)
        case false => 1.0
      }

      val (src, dst) = (parts.head, parts(1))
      if (bcDirected.value) {
        Array((src, dst, weight))
      } else {
        Array((src, dst, weight), (dst, src, weight))
      }
    }

    // Whether it needs to assign a long type unique ids to nodes or not.
    val triplets = config.indexed match {
      case true => inputTriplets.map { case (src, dst, weight) => (src.toLong, dst.toLong, weight) }
      case false =>
        val (label2id_, indexedTriplets) = indexingNode(inputTriplets)
        this.label2id = label2id_
        indexedTriplets
    }


    // tO(m) + aO(m) + ~tO(n+m) + tO(n) shuffling for repartitioning
    val bcMaxDegree = context.broadcast(config.degree)
    val node2attr = triplets.map { case (src, dst, weight) =>
      (src, Array((dst, weight)))
    }.reduceByKey(_ ++ _).map { case (srcId, neighbors: Array[(Long, Double)]) =>
      // Why this neighbors_ is required?
      var neighbors_ : Array[(Long, Double)] = neighbors.groupBy(_._1).map { case (group,
      traversable) =>
        traversable.head
      }.toArray
      if (neighbors_.length > bcMaxDegree.value) {
        // Sort neighbors based on their weight in a descending order
        // It takes the first bcMaxDegree number of neighbors into account and remove the rest
        // from the neighbors for the nodes having higher degree than bcMaxDegree.
        neighbors_ = neighbors.sortWith { case (left, right) => left._2 > right._2 }.slice(0,
          bcMaxDegree.value)
      }

      (srcId, NodeAttr(neighbors = neighbors_))
    }.repartition(200).cache


    // Creates an Array of GraphX Edge objects with the EdgeAttr objects attached to each edge
    // tO(n+m)
    // Repartition shuffles tO(m)
    val edge2attr = node2attr.flatMap { case (srcId, clickNode) =>
      clickNode.neighbors.map { case (dstId, weight) =>
        Edge(srcId, dstId, EdgeAttr())
      }
    }.repartition(200).cache

    // This returns an object of graphx Graph class
    GraphOps.initTransitionProb(node2attr, edge2attr)
  }

  def randomWalk(g: Graph[NodeAttr, EdgeAttr]) = {
    // create a list of edges in the format of (srcIdDstId, edgeAttr) and partitions them based
    // on hash of srcIdDstId.
    val edge2attr = g.triplets.map { edgeTriplet =>
      (s"${edgeTriplet.srcId}${edgeTriplet.dstId}", edgeTriplet.attr)
    }.reduceByKey { case (l, r) => l }.partitionBy(new HashPartitioner(200)).persist(StorageLevel
      .MEMORY_ONLY) // What is actually reducebykey doing?
    logger.info(s"edge2attr: ${edge2attr.count}")

    val examples = g.vertices.cache // Why is it called examples?
    logger.info(s"examples: ${examples.count}")

    g.unpersist(blocking = false)
    g.edges.unpersist(blocking = false)
    g.vertices.unpersist(blocking = false)

    var totalRandomPath: RDD[String] = null // Includes all the random walks in the format of
    // tab-separated vertex ids per tuple. It contains config.numWalks number of walks per vertex.

    for (iter <- 0 until config.numWalks) {
      var prevRandomPath: RDD[String] = null
      var randomPath: RDD[String] = examples.map { case (nodeId, clickNode) =>
        clickNode.path.mkString("\t")
      }.cache // clickNode is NodeAttr. Makes a tab-separated string of path elements.
      var activeWalks = randomPath.first // not used?!
      // For the length of walks for every vertex, do walk in parallel. Each vertex, keeps the path of its own.
      for (walkCount <- 0 until config.walkLength) {
        prevRandomPath = randomPath
        // Join the last of edge of each path with its attribute
        randomPath = edge2attr.join(randomPath.mapPartitions { iter =>
          iter.map { pathBuffer =>
            val paths = pathBuffer.split("\t") // Get the path nodes
            // The last two nodes of the path and concat them. make a (lastEdgeOfThePath, path)
            (paths.slice(paths.size - 2, paths.size).mkString(""), pathBuffer)
          }
        }).mapPartitions { iter =>
          iter.map { case (edge, (attr, pathBuffer)) =>
            try {
              if (pathBuffer != null && pathBuffer.nonEmpty) {
                val nextNodeIndex = GraphOps.drawAlias(attr.J, attr.q) // biased random selection
                // of the next node
                val nextNodeId = attr.dstNeighbors(nextNodeIndex)

                s"$pathBuffer\t$nextNodeId"
              } else {
                null
              }
            } catch {
              case e: Exception => throw new RuntimeException(e.getMessage)
            }
          }.filter(_ != null)
        }.cache

        activeWalks = randomPath.first // Not used?!
        prevRandomPath.unpersist(blocking = false)
      }

      if (totalRandomPath != null) {
        val prevRandomWalkPaths = totalRandomPath
        totalRandomPath = totalRandomPath.union(randomPath).cache()
        totalRandomPath.count
        prevRandomWalkPaths.unpersist(blocking = false)
      } else {
        totalRandomPath = randomPath
      }
    }

    totalRandomPath
  }

  /* It converts arbitrary node ids to long type node ids between [0,NodeSize]. It also creates a
    Mapping of the original node ids to the new node ids.
    |E| = m
    |V| = n
  */
  def indexingNode(triplets: RDD[(String, String, Double)]) = {
    // 3 * O(m)
    val label2id = createNode2Id(triplets) // (string-node-id, unique-long-id)

    // 4 * O(m) + 2 * o(m*n)
    val indexedTriplets = triplets.map { case (src, dst, weight) =>
      (src, (dst, weight))
    }.join(label2id).map { case (src, (edge: (String, Double), srcIndex: Long)) =>
      try {
        val (dst: String, weight: Double) = edge
        (dst, (srcIndex, weight))
      } catch {
        case e: Exception => null
      }
    }.filter(_ != null).join(label2id).map { case (dst, (edge: (Long, Double), dstIndex: Long)) =>
      try {
        val (srcIndex, weight) = edge
        (srcIndex, dstIndex, weight)
      } catch {
        case e: Exception => null
      }
    }.filter(_ != null)

    (label2id, indexedTriplets)
  }


  def createNode2Id[T <: Any](triplets: RDD[(String, String, T)]): RDD[(String, Long)] = triplets
    .flatMap { case (src, dst, weight) =>
      Try(Array(src, dst)).getOrElse(Array.empty[String])
    }.distinct().zipWithIndex()

  def save(randomPaths: RDD[String]): this.type = {
    randomPaths.filter(x => x != null && x.replaceAll("\\s", "").length > 0)
      .repartition(200)
      .saveAsTextFile(s"${config.output}.${Property.pathSuffix}")

    if (Some(this.label2id).isDefined) {
      label2id.map { case (label, id) =>
        s"$label\t$id"
      }.saveAsTextFile(s"${config.output}.${Property.node2idSuffix}")
    }

    this
  }

}
