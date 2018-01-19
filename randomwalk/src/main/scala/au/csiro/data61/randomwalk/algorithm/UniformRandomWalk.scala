package au.csiro.data61.randomwalk.algorithm

import au.csiro.data61.randomwalk.common.Params
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.util.Try

case class UniformRandomWalk(context: SparkContext, config: Params) extends RandomWalk {

  def prepareForAliasSampling(edges: RDD[(Int, Array[(Int, Float)])]) = {
    val neighbors = edges.reduceByKey(_ ++ _).partitionBy(partitioner).cache()
    edges.map { case (src, edge) =>
      val dst = edge(0)._1
      val w = edge(0)._2
      (dst, (src, w))
    }.partitionBy(partitioner).join(neighbors).map { case (dst, ((src, w), dstNeighbors)) =>
      (src, (dst, w, dstNeighbors))
    }.map { case (src, (dst, w, dstNeighbors)) =>
      (src, Array((dst, w, dstNeighbors)))
    }.reduceByKey(_ ++ _)
  }

  def buildRoutingTableWithAlias(graph: RDD[(Int, Array[(Int, Float, Array[(Int, Float)])])])
  : RDD[Int] = {
    val bcP = context.broadcast(config.p)
    val bcQ = context.broadcast(config.q)
    graph.mapPartitionsWithIndex({
      (id: Int, iter: Iterator[(Int, Array[(Int, Float, Array[(Int, Float)])])]) =>
        iter.foreach { case (vId, neighbors) =>
          GraphMap.addVertex(p = bcP.value.toFloat, q = bcQ.value.toFloat)(vId, neighbors)
          id
        }
        Iterator.empty
    }, preservesPartitioning = true
    )
  }

  /**
    * Loads the graph and computes the probabilities to go from each vertex to its neighbors
    *
    * @return
    */
  def loadGraph(): RDD[(Int, Array[Int])] = {
    // the directed and weighted parameters are only used for building the graph object.
    // is directed? they will be shared among stages and executors
    val bcDirected = context.broadcast(config.directed)
    val bcWeighted = context.broadcast(config.weighted) // is weighted?

    val edges: RDD[(Int, Array[(Int, Float)])] = context.textFile(config.input, minPartitions
      = config
      .rddPartitions).flatMap { triplet =>
      val parts = triplet.split("\\s+")
      // if the weights are not specified it sets it to 1.0

      val weight = bcWeighted.value && parts.length > 2 match {
        case true => Try(parts.last.toFloat).getOrElse(1.0f)
        case false => 1.0f
      }

      val (src, dst) = (parts.head.toInt, parts(1).toInt)
      if (bcDirected.value) {
        Array((src, Array((dst, weight))), (dst, Array.empty[(Int, Float)]))
      } else {
        Array((src, Array((dst, weight))), (dst, Array((src, weight))))
      }
    }

    val g = config.aliasSampling match {
      case false =>
        val graph = edges.reduceByKey(_ ++ _).partitionBy(partitioner).persist(StorageLevel
          .MEMORY_AND_DISK)
        routingTable = buildRoutingTable(graph).persist(StorageLevel.MEMORY_ONLY)
        graph
      case true =>
        val graph = prepareForAliasSampling(edges).persist(StorageLevel.MEMORY_AND_DISK)
        routingTable = buildRoutingTableWithAlias(graph).persist(StorageLevel.MEMORY_ONLY)
        graph
    }


    routingTable.count()

    val vAccum = context.longAccumulator("vertices")
    val eAccum = context.longAccumulator("edges")

    val rAcc = context.collectionAccumulator[Int]("replicas")
    val lAcc = context.collectionAccumulator[Int]("links")

    g.foreachPartition { iter =>
      val (r, e) = GraphMap.getGraphStatsOnlyOnce
      if (r != 0) {
        rAcc.add(r)
        lAcc.add(e)
      }
      iter.foreach {
        case (_, (neighbors)) =>
          vAccum.add(1)
          eAccum.add(neighbors.length)
      }
    }
    nVertices = vAccum.sum.toInt
    nEdges = eAccum.sum.toInt

    logger.info(s"edges: $nEdges")
    logger.info(s"vertices: $nVertices")
    println(s"edges: $nEdges")
    println(s"vertices: $nVertices")

    val ePartitions = lAcc.value.toArray.mkString(" ")
    val vPartitions = rAcc.value.toArray.mkString(" ")
    logger.info(s"E Partitions: $ePartitions")
    logger.info(s"V Partitions: $vPartitions")
    println(s"E Partitions: $ePartitions")
    println(s"V Partitions: $vPartitions")

    g.mapPartitions({ iter =>
      iter.map {
        case (vId: Int, _) =>
          (vId, Array(vId))
      }
    }, preservesPartitioning = true
    )
  }

  def buildRoutingTable(graph: RDD[(Int, Array[(Int, Float)])]): RDD[Int] = {

    graph.mapPartitionsWithIndex({ (id: Int, iter: Iterator[(Int, Array[(Int, Float)])]) =>
      iter.foreach { case (vId, neighbors) =>
        GraphMap.addVertex(vId, neighbors)
        id
      }
      Iterator.empty
    }, preservesPartitioning = true
    )

  }

  def prepareWalkersToTransfer(walkers: RDD[(Int, (Array[Int], Array[(Int, Float)], Boolean))]) = {
    walkers.mapPartitions({
      iter =>
        iter.map {
          case (_, (steps, prevNeighbors, completed)) => (steps.last, (steps, prevNeighbors,
            completed))
        }
    }, preservesPartitioning = false)

  }

  def prepareWalkersToTransferForAliasWalk(walkers: RDD[(Int, (Array[Int], Boolean))]) = {
    walkers.mapPartitions({
      iter =>
        iter.map {
          case (_, (steps, completed)) => (steps.last, (steps, completed))
        }
    }, preservesPartitioning = false)

  }

}
