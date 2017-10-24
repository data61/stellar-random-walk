package com.navercorp

import java.io.Serializable

import org.apache.log4j.LogManager

import scala.util.Try
//import org.slf4j.{LoggerFactory, Logger}
import org.apache.spark.{SparkContext, HashPartitioner}
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.{Graph, _}
import org.apache.spark.storage.StorageLevel
import com.navercorp.graph.{GraphOps, EdgeAttr, NodeAttr}
import com.navercorp.common.Property

object Node2vec extends Serializable {
//  lazy val logger: Logger = LoggerFactory.getLogger(getClass.getName)
  lazy val logger = LogManager.getLogger("myLogger")
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
    val node2attr = context.textFile(config.input).flatMap { triplet =>
      val parts = triplet.split("\\s")
      // if the weights are not specified it sets it to 1.0
      val weight = bcWeighted.value match {
        case true => Try(parts.last.toDouble).getOrElse(1.0)
        case false => 1.0
      }

      val (src, dst) = (parts.head.toLong, parts(1).toLong)
      if (bcDirected.value) {
        Array((src, Array((dst, weight))))
      } else {
        Array((src, Array((dst, weight))), (dst, Array((src, weight))))
      }
    }.reduceByKey(_ ++ _).map { case (srcId, neighbors: Array[(Long, Double)]) =>
      (srcId, NodeAttr(neighbors))
    }.repartition(config.rddPartitions).cache


    // Creates an Array of GraphX Edge objects with the EdgeAttr objects attached to each edge
    // tO(n+m)
    // Repartition shuffles tO(m)
    val edge2attr = node2attr.flatMap { case (srcId, clickNode) =>
      clickNode.neighbors.map { case (dstId, weight) =>
        Edge(srcId, dstId, EdgeAttr())
      }
    }.repartition(config.rddPartitions).cache

    // This returns an object of graphx Graph class
    GraphOps.initTransitionProb(node2attr, edge2attr)
  }

  def randomWalk(g: Graph[NodeAttr, EdgeAttr]) = {
    // create a list of edges in the format of (srcIdDstId, edgeAttr) and partitions them based
    // on hash of srcIdDstId.
    val edge2attr = g.triplets.map { edgeTriplet =>
      (s"${edgeTriplet.srcId}${edgeTriplet.dstId}", edgeTriplet.attr)
    }.partitionBy(new HashPartitioner(config.rddPartitions)).persist(StorageLevel
      .MEMORY_ONLY)

    logger.info(s"edge2attr: ${edge2attr.count}")

    val examples = g.vertices.cache // Why is it called examples?
    logger.info(s"examples: ${examples.count}")

    g.unpersist(blocking = false)
    g.edges.unpersist(blocking = false)
    g.vertices.unpersist(blocking = false)

    var totalRandomPath: RDD[String] = null // Includes all the random walks in the format of
    // tab-separated vertex ids per tuple. It contains config.numWa;l

    for (iter <- 0 until config.numWalks) {
      var prevRandomPath: RDD[String] = null
      var randomPath: RDD[String] = examples.map { case (nodeId, clickNode) =>
        clickNode.path.mkString("\t")
      }.cache // clickNode is NodeAttr.
      // Makes a tab-separated string of path elements.

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

  def save(randomPaths: RDD[String]): this.type = {
    randomPaths.filter(x => x != null && x.replaceAll("\\s", "").length > 0)
      .repartition(config.rddPartitions)
      .saveAsTextFile(s"${config.output}.${Property.pathSuffix}")

    this
  }

}
