package au.csiro.data61

import com.navercorp.common.Property
import org.apache.log4j.LogManager
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{HashPartitioner, SparkContext}

import scala.util.{Random, Try}

case class RandomWalk2(context: SparkContext,
                       config: Main.Params) extends Serializable {

  lazy val logger = LogManager.getLogger("myLogger")
  val gMap = context.broadcast(GraphMap).value

  /**
    * Loads the graph and computes the probabilities to go from each vertex to its neighbors
    *
    * @return
    */
  def loadGraph(): RDD[Array[Long]] = {
    // the directed and weighted parameters are only used for building the graph object.
    // is directed? they will be shared among stages and executors
    val bcDirected = context.broadcast(config.directed)
    val bcWeighted = context.broadcast(config.weighted) // is weighted?
    // inputTriplets is an array of edges (src, dst, weight).

    val edges: RDD[Edge[Double]] = context.textFile(config.input).flatMap { triplet =>
      val parts = triplet.split("\\s+")
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

    val graph: Graph[_, Double] = Graph.fromEdges(edges, defaultValue = None,
      edgeStorageLevel =
        StorageLevel.MEMORY_ONLY, vertexStorageLevel = StorageLevel.MEMORY_ONLY).
      partitionBy(partitionStrategy = PartitionStrategy.EdgePartition2D, numPartitions = config
        .rddPartitions).cache()

    val g = graph.collectEdges(EdgeDirection.Out).rightOuterJoin(graph.vertices).map { case
      (vId: Long, (neighbors: Option[Array[Edge[Double]]], _)) =>
      neighbors match {
        case Some(edges) => (vId, edges)
        case None => (vId, Array.empty[Edge[Double]])
      }
    }

    gMap.setUp(g.collect(), graph.edges.count.toInt)

    logger.info(s"edges: ${gMap.numEdges}")
    logger.info(s"vertices: ${gMap.numVertices}")

    val initPaths = graph.vertices.map { case (vId: Long, _) =>
      (Array(vId))
    }

    initPaths
  }

  def doFirsStepOfRandomWalk(paths: RDD[Array[Long]], nextDouble: () =>
    Double = Random.nextDouble): RDD[Array[Long]] = {
    paths.map { case (path: Array[Long]) =>
      val gMap = context.broadcast(GraphMap).value
      val neighbors = gMap.getNeighbors(path.head)
      if (neighbors.length > 0) {
        val (nextStep, _) = RandomSample(nextDouble).sample(neighbors)
        path ++ Array(nextStep)
      }

      path
    }
  }

//  def randomWalk(g: Graph[Array[Long], Double], nextDoubleGen: () => Double = Random.nextDouble)
//  : RDD[Array[Long]] = {
//    val bcP = context.broadcast(config.p)
//    val bcQ = context.broadcast(config.q)
//    val walkLength = context.broadcast(config.walkLength).value
//    //    val nextDouble = context.broadcast(nextDoubleGen).value
//    // initialize the first step of the random walk
//
//    val v2e = g.collectEdges(EdgeDirection.Out).partitionBy(new HashPartitioner(config
//      .rddPartitions)).cache()
//    //    val v2e = g.collectEdges(EdgeDirection.Out).repartition(config.rddPartitions).cache()
//    var v2p = doFirsStepOfRandomWalk(v2e, g, nextDoubleGen)
//      .repartition(config.rddPartitions)
//      .cache()
//    for (walkCount <- 0 until walkLength) {
//      v2p = v2e.rightOuterJoin(v2p).mapPartitions(iter => {
//        iter.map {
//          case (currId,
//          (currNeighbors, (
//            (prevId, prevNeighbors), path))) =>
//            if (currId == prevId)
//              (currId, ((currId, currNeighbors), path)) // This can be more optimized
//            else {
//              currNeighbors match {
//                case Some(edges) =>
//                  RandomSample(nextDoubleGen).secondOrderSample(bcP.value, bcQ.value)(prevId,
//                    prevNeighbors,
//                    edges)
//                  match {
//                    case Some(newStep) => (newStep.dstId, ((currId, currNeighbors),
//                      path ++ Array(newStep.dstId)))
//                    //                case None => (currId, ((currId, currNeighbors), path)) // This
//                    // can be
//
//                    //                // filtered in future.
//                  }
//                case None => (currId, ((currId, currNeighbors), path))
//
//              }
//            }
//        }
//      }
//        , preservesPartitioning = false
//      ).cache()
//    }
//
//    v2p.map { case (_, ((_, _), path)) =>
//      path
//    }
//  }

  def save(paths: RDD[Array[Long]]) = {

    paths.map {
      case (path) =>
        val pathString = path.mkString("\t")
        s"$pathString"
    }.repartition(numPartitions = config.rddPartitions).saveAsTextFile(s"${
      config.output
    }" +
      s".${
        Property.pathSuffix
      }")
  }

}
