package au.csiro.data61

import com.navercorp.common.Property
import org.apache.log4j.LogManager
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, EdgeDirection, Graph, PartitionStrategy}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.util.{Random, Try}

case class RandomWalk(context: SparkContext,
                      config: Main.Params) extends Serializable {

  lazy val logger = LogManager.getLogger("myLogger")

  /**
    * Loads the graph and computes the probabilities to go from each vertex to its neighbors
    *
    * @return
    */
  def loadGraph(): Graph[Array[Long], Double] = {
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

    val graph: Graph[Array[Long], Double] = Graph.fromEdges(edges, defaultValue = Array.empty[Long],
      edgeStorageLevel =
        StorageLevel.MEMORY_ONLY, vertexStorageLevel = StorageLevel.MEMORY_ONLY).
      partitionBy(partitionStrategy = PartitionStrategy.EdgePartition2D, numPartitions = config
        .rddPartitions).cache()

    logger.info(s"edges: ${graph.edges.count}")
    logger.info(s"vertices: ${graph.vertices.count}")

    graph
  }

  def randomWalk(g: Graph[Array[Long], Double], nextDoubleGen: () => Double = Random.nextDouble)
  : RDD[Array[Long]] = {
    val bcP = context.broadcast(config.p)
    val bcQ = context.broadcast(config.q)
    val walkLength = context.broadcast(config.walkLength).value
    //    val nextDouble = context.broadcast(nextDoubleGen).value
    // initialize the first step of the random walk
    var v2p = doFirsStepOfRandomWalk(g, nextDoubleGen).cache()

    for (walkCount <- 0 until walkLength) {
      v2p = g.collectEdges(EdgeDirection.Out).rightOuterJoin(v2p).map { case (currId,
      (currNeighbors, (
        (prevId, prevNeighbors), path))) =>
        if (currId == prevId)
          (currId, ((currId, currNeighbors), path)) // This can be more optimized
        else {
          currNeighbors match {
            case Some(edges) =>
              RandomSample(nextDoubleGen).secondOrderSample(bcP.value, bcQ.value)(prevId,
                prevNeighbors,
                edges)
              match {
                case Some(newStep) => (newStep.dstId, ((currId, currNeighbors),
                  path ++ Array(newStep.dstId)))
                //                case None => (currId, ((currId, currNeighbors), path)) // This
                // can be

                //                // filtered in future.
              }
            case None => (currId, ((currId, currNeighbors), path))
          }
        }
      }.cache()
    }


    v2p.map { case (_, ((_, _), path)) =>
      path
    }
  }

  def doFirsStepOfRandomWalk(g: Graph[Array[Long], Double], nextDouble: () => Double = Random
    .nextDouble): RDD[(Long,
    ((Long, Option[Array[Edge[Double]]]), Array[Long]))] = {
    g.collectEdges(EdgeDirection.Out).rightOuterJoin(g.vertices).map { case (currId: Long,
    (currNeighbors: Option[Array[Edge[Double]]], path: Array[Long])) =>
      currNeighbors match {
        case Some(edges) =>
          RandomSample(nextDouble).sample(edges) match {
            case Some(newStep) => (newStep.dstId, ((currId, Some(edges)), path
              ++ Array(currId, newStep.dstId)))
            //            case None => (currId, ((currId, edges), path ++ Array(currId))) // This
            // can be
            // filtered in future.
          }
        case None => (currId, ((currId, None), path ++ Array(currId)))
      }
    }
  }

  def save(paths: RDD[Array[Long]]) = {

    paths.map { case (path) =>
      val pathString = path.mkString("\t")
      s"$pathString"
    }.repartition(numPartitions = config.rddPartitions).saveAsTextFile(s"${config.output}" +
      s".${Property.pathSuffix}")
  }

}
