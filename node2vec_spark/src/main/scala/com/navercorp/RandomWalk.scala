package com.navercorp

import com.navercorp.common.Property
import com.navercorp.graph2.RandomSample._
import org.apache.log4j.LogManager
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, EdgeDirection, Graph, PartitionStrategy}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.util.Try

object RandomWalk {

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
  def loadGraph():Graph[Array[Long], Double] = {
    // the directed and weighted parameters are only used for building the graph object.
    // is directed? they will be shared among stages and executors
    val bcDirected = context.broadcast(config.directed)
    val bcWeighted = context.broadcast(config.weighted) // is weighted?
    // inputTriplets is an array of edges (src, dst, weight).

    val edges: RDD[Edge[Double]] = context.textFile(config.input).flatMap { triplet =>
      val parts = triplet.split("\\s")
      // if the weights are not specified it sets it to 1.0
      val weight = bcWeighted.value match {
        case true => Try(parts.last.toDouble).getOrElse(1.0)
        case false => 1.0
      }

      val (src, dst) = (parts.head.toLong, parts(1).toLong)
      if (bcDirected.value) {
        Array(Edge(src, dst, weight))
      } else {
        Array(Edge(src, dst, weight), Edge(dst, src, weight))
      }
    }

    val graph: Graph[Array[Long], Double] = Graph.fromEdges(edges, defaultValue = Array.empty[Long],
      edgeStorageLevel =
        StorageLevel.MEMORY_ONLY, vertexStorageLevel = StorageLevel.MEMORY_ONLY).
      partitionBy(partitionStrategy = PartitionStrategy.EdgePartition2D, numPartitions = config
        .rddPartitions).cache()

    logger.info(s"edges: ${graph.edges.count}")
    logger.info(s"vertices: ${graph.vertices.count}")

    graph
  }

  def randomWalk(g: Graph[Array[Long], Double]): RDD[String] = {
    val bcP = context.broadcast(config.p)
    val bcQ = context.broadcast(config.q)

    // initialize the first step of the random walk
    var v2p = g.collectEdges(EdgeDirection.Out).join(g.vertices).map { case (currId: Long,
    (currNeighbors: Array[Edge[Double]], path: Array[Long])) =>
      sample(currNeighbors) match {
        case Some(newStep) => (newStep.dstId, ((currId, currNeighbors), path ++ Array(currId,
          newStep.dstId)))
        case None => (currId, ((currId, currNeighbors), path ++ Array(currId))) // This can be
        // filtered in future.
      }
    }.cache()

    for (walkCount <- 0 until config.walkLength) {
      v2p = g.collectEdges(EdgeDirection.Out).join(v2p).map { case (currId, (currNeighbors, (
        (prevId, prevNeighbors), path))) =>
        if (currId == prevId)
          (currId, ((currId, currNeighbors), path)) // This can be more optimized
        else
          secondOrderSample(bcP.value, bcQ.value)(prevId, prevNeighbors, currNeighbors) match {
            case Some(newStep) => (newStep.dstId, ((currId, currNeighbors), path ++ Array(newStep
              .dstId)))
            case None => (currId, ((currId, currNeighbors), path)) // This can be
            // filtered in future.
          }
      }.cache()
    }


    v2p.map { case (_, ((_, _), path)) =>
      val pathString = path.mkString("\t")
      s"$pathString"
    }
  }

  def save(paths: RDD[String]) = {
    paths.repartition(numPartitions = config.rddPartitions).saveAsTextFile(s"${config.output}" +
      s".${Property.pathSuffix}")
  }

}
