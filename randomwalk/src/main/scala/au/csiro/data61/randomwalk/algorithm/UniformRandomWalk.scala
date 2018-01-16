package au.csiro.data61.randomwalk.algorithm

import au.csiro.data61.randomwalk.common.Params
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.util.Try

case class UniformRandomWalk(context: SparkContext, config: Params) extends RandomWalk {

  /**
    * Loads the graph and computes the probabilities to go from each vertex to its neighbors
    *
    * @return
    */
  def loadGraph(hetero: Boolean, bcMetapath: Broadcast[Array[Short]]): RDD[(Int, Array[Int])] = {
    // the directed and weighted parameters are only used for building the graph object.
    // is directed? they will be shared among stages and executors
    val g = hetero match {
      case true => loadHeteroGraph().partitionBy(partitioner).persist(StorageLevel.MEMORY_AND_DISK)
      case false => loadHomoGraph().partitionBy(partitioner).persist(StorageLevel.MEMORY_AND_DISK)
    }

    // the size must be compatible with the vertex type ids (from 0 to vTypeSize - 1)
    HGraphMap.initGraphMap(config.vTypeSize)

    routingTable = buildRoutingTable(g).persist(StorageLevel.MEMORY_ONLY)
    routingTable.count()

    val vAccum = context.longAccumulator("vertices")
    val eAccum = context.longAccumulator("edges")

    val rAcc = context.collectionAccumulator[Int]("replicas")
    val lAcc = context.collectionAccumulator[Int]("links")

    g.foreachPartition { iter =>
      val (r, e) = HGraphMap.getGraphStatsOnlyOnce
      if (r != 0) {
        rAcc.add(r)
        lAcc.add(e)
      }
      iter.foreach {
        case (_, (edgeTypes, _)) =>
          vAccum.add(1)
          edgeTypes.foreach { case (neighbors: Array[(Int, Float)], _) =>
            eAccum.add(neighbors.length)
          }
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

    g.filter(v => v._2._2 == bcMetapath.value(0)).mapPartitions({ iter =>
      iter.map {
        case (vId: Int, _) =>
          (vId, Array(vId))
      }
    }, preservesPartitioning = true
    )
  }

  def loadHomoGraph(): RDD[(Int, (Array[(Array[(Int, Float)], Short)], Short))] = {
    val bcDirected = context.broadcast(config.directed)
    val bcWeighted = context.broadcast(config.weighted) // is weighted?

    context.textFile(config.input, minPartitions
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
    }.reduceByKey(_ ++ _).map { case (src, neighbors) =>
      val defaultNodeType: Short = 0
      (src, (Array((neighbors, defaultNodeType)), defaultNodeType))
    }
  }

  def loadHeteroGraph(): RDD[(Int, (Array[(Array[(Int, Float)], Short)], Short))] = {
    val bcDirected = context.broadcast(config.directed)
    val bcWeighted = context.broadcast(config.weighted) // is weighted?

    val edges = context.textFile(config.input, minPartitions = config.rddPartitions).flatMap {
      triplet =>
        val parts = triplet.split("\\s+")
        // if the weights are not specified it sets it to 1.0

        val weight = bcWeighted.value && parts.length > 2 match {
          case true => Try(parts.last.toFloat).getOrElse(1.0f)
          case false => 1.0f
        }

        val (src, dst) = (parts.head.toInt, parts(1).toInt)

        if (bcDirected.value) {
          //        Array((dst, (src, weight)), (None, (dst, None)))
          Array((dst, (src, weight)))
        } else {
          Array((dst, (src, weight)), (src, (dst, weight)))
        }
      /* TODO: Check for input data correctness: e.g., no redundant edges, no bidirectional
      representations, vertex-type exists for all vertices, and so on*/
    }.partitionBy(partitioner)

    val vTypes = loadNodeTypes().partitionBy(partitioner).cache()
    appendNodeTypes(edges, vTypes, bcDirected).reduceByKey(_ ++ _).map {
      case ((src, dstType), neighbors) => (src, Array((neighbors, dstType)))
    }.reduceByKey(_ ++ _).join(vTypes, partitioner)
  }

  def appendNodeTypes(reversedEdges: RDD[(Int, (Int, Float))], vTypes: RDD[(Int, Short)],
                      bcDirected: Broadcast[Boolean]):
  RDD[((Int, Short), Array[(Int, Float)])] = {
    return reversedEdges.join(vTypes).flatMap { case (dst, ((src, weight), dstType)) =>
      val v = Array(((src, dstType), Array((dst, weight))))
      if (bcDirected.value) {
        v ++ Array(((dst, dstType), Array.empty[(Int, Float)]))
      } else {
        v
      }
    }
  }

  def buildRoutingTable(graph: RDD[(Int, (Array[(Array[(Int, Float)], Short)], Short))])
  : RDD[Int] = {

    graph.mapPartitionsWithIndex({ (id: Int, iter: Iterator[(Int, (Array[(Array[(Int, Float)],
      Short)], Short))]) =>
      iter.foreach { case (vId, (edgeTypes, _)) =>
        edgeTypes.foreach { case (neighbors, dstType) =>
          HGraphMap.addVertex(dstType, vId, neighbors)
        }
        id
      }
      Iterator.empty
    }, preservesPartitioning = true
    )

  }

  def prepareWalkersToTransfer(walkers: RDD[(Int, (Array[Int], Array[(Int, Float)], Boolean,
    Short))]) = {
    walkers.mapPartitions({
      iter =>
        iter.map {
          case (_, (steps, prevNeighbors, completed, mpIndex)) => (steps.last, (steps,
            prevNeighbors, completed, mpIndex))
        }
    }, preservesPartitioning = false)

  }

}
