package au.csiro.data61.randomwalk.efficient

import au.csiro.data61.Main
import com.navercorp.common.Property
import org.apache.log4j.LogManager
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{HashPartitioner, SparkContext}

import scala.util.control.Breaks._
import scala.util.{Random, Try}

case class RandomWalk(context: SparkContext,
                      config: Main.Params) extends Serializable {

  lazy val logger = LogManager.getLogger("myLogger")
  val gMap = context.broadcast(GraphMap())

  /**
    * Loads the graph and computes the probabilities to go from each vertex to its neighbors
    *
    * @return
    */
  def loadGraph(): RDD[(Long, Array[Long])] = {
    // the directed and weighted parameters are only used for building the graph object.
    // is directed? they will be shared among stages and executors
    val bcDirected = context.broadcast(config.directed)
    val bcWeighted = context.broadcast(config.weighted) // is weighted?
    // inputTriplets is an array of edges (src, dst, weight).

    val edges: RDD[Edge[Double]] = context.textFile(config.input).flatMap { triplet =>
      val parts = triplet.split("\\s+")
      // if the weights are not specified it sets it to 1.0

      val weight = bcWeighted.value && parts.length > 2 match {
        case true => Try(parts.last.toDouble).getOrElse(1.0)
        case false => 1.0
      }

      val (src, dst) = (parts.head.toLong, parts(1).toLong)
      if (bcDirected.value) {
        Array(Edge(src, dst, weight))
      } else {
        Array(Edge(src, dst, weight), Edge(dst, src, weight))
      }
    }.cache()

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

    gMap.value.setUp(g.collect(), graph.edges.count.toInt)

    logger.info(s"edges: ${gMap.value.numEdges}")
    logger.info(s"vertices: ${gMap.value.numVertices}")

    val initPaths = graph.vertices.map { case (vId: Long, _) =>
      (vId, Array(vId))
    }

    graph.unpersist(blocking = false)
    edges.unpersist(blocking = false)

    initPaths.partitionBy(new HashPartitioner(config.rddPartitions)).cache()
  }

  def doFirsStepOfRandomWalk(paths: RDD[(Long, Array[Long])], nextDouble: () =>
    Double = Random.nextDouble): RDD[(Long, Array[Long])] = {
    val map = gMap.value
    paths.mapPartitions { iter =>
      iter.map { case ((src: Long, path: Array[Long])) =>
        val neighbors = map.getNeighbors(path.head)
        if (neighbors.length > 0) {
          val (nextStep, _) = RandomSample(nextDouble).sample(neighbors)
          (src, path ++ Array(nextStep))
        } else {
          (src, path)
        }
      }
    }
  }

  def randomWalk(initPaths: RDD[(Long, Array[Long])], nextDouble: () => Double = Random
    .nextDouble)
  : RDD[Array[Long]] = {
    val bcP = context.broadcast(config.p)
    val bcQ = context.broadcast(config.q)
    val walkLength = context.broadcast(config.walkLength).value
    val numberOfWalks = context.broadcast(config.numWalks).value
    // initialize the first step of the random walk
    var totalPaths: RDD[Array[Long]] = null
    val map = gMap.value
    val paths = doFirsStepOfRandomWalk(initPaths, nextDouble)
    for (_ <- 0 until numberOfWalks) {
      val newPaths = paths.mapPartitions { iter =>
        iter.map { case (_, firstStep: Array[Long]) =>
          var path = firstStep
          val rSample = RandomSample(nextDouble)
          if (firstStep.length > 1)
            breakable {
              for (_ <- 0 until walkLength) {
                val curr = path.last
                val currNeighbors = map.getNeighbors(curr)
                if (currNeighbors.length > 0) {
                  val prev = path(path.length - 2)
                  val prevNeighbors = map.getNeighbors(prev)
                  val (nextStep, _) = rSample.secondOrderSample(bcP.value, bcQ
                    .value, prev, prevNeighbors, currNeighbors)
                  path = path ++ Array(nextStep)
                } else {
                  break
                }
              }
            }
          path
        }
      }.cache()

      if (totalPaths != null)
        totalPaths = totalPaths.union(newPaths).cache()
      else
        totalPaths = newPaths
    }

    totalPaths
  }

  def save(paths: RDD[Array[Long]]) = {

    paths.mapPartitions { iter =>
      iter.map {
        case (path) =>
          val pathString = path.mkString("\t")
          s"$pathString"
      }
    }.repartition(config.rddPartitions).saveAsTextFile(s"${
      config.output
    }" +
      s".${
        Property.pathSuffix
      }")
  }

}
