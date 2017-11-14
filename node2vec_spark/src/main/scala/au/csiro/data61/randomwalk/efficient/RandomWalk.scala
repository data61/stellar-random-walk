package au.csiro.data61.randomwalk.efficient

import au.csiro.data61.Main
import com.navercorp.common.Property
import org.apache.log4j.LogManager
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{HashPartitioner, SparkContext}

import scala.collection.immutable.Stream.Empty
import scala.util.control.Breaks._
import scala.util.{Random, Try}

case class RandomWalk(context: SparkContext,
                      config: Main.Params) extends Serializable {

  lazy val logger = LogManager.getLogger("myLogger")
  var nVertices: Long = 0
  var nEdges: Long = 0

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
    val vAccum = context.longAccumulator("vertices")
    val eAccum = context.longAccumulator("edges")

    val g: RDD[(Long, Array[(Long, Double)])] = context.textFile(config.input, minPartitions
      = config
      .rddPartitions).flatMap { triplet =>
      val parts = triplet.split("\\s+")
      // if the weights are not specified it sets it to 1.0

      val weight = bcWeighted.value && parts.length > 2 match {
        case true => Try(parts.last.toDouble).getOrElse(1.0)
        case false => 1.0
      }

      val (src, dst) = (parts.head.toLong, parts(1).toLong)
      if (bcDirected.value) {
        eAccum.add(1)
        Array((src, Array((dst, weight))), (dst, Array.empty[(Long, Double)]))
      } else {
        eAccum.add(2)
        Array((src, Array((dst, weight))), (dst, Array((src, weight))))
      }
    }.
      reduceByKey(_ ++ _).
      partitionBy(new HashPartitioner(config.rddPartitions)).
      persist(StorageLevel.MEMORY_AND_DISK) // TODO: Apply a smart graph partition strategy

    val numDeadEnds = context.broadcast(g.filter(_._2.isEmpty).count().toInt)
    val numEdges = context.broadcast(eAccum.sum.toInt)
    eAccum.reset()

    val numVertices = context.broadcast(g.count().toInt)

    g.mapPartitions { iter =>
      GraphMap.setUp(numVertices.value, numDeadEnds.value, numEdges.value)
      val newIter = iter.map {
        case (vId: Long, (neighbors: Array[(Long, Double)])) =>
          GraphMap.addVertex(vId, neighbors)
          vAccum.add(1)
          eAccum.add(neighbors.length)
          vId
      }
      newIter
    }.count()

    nVertices = vAccum.sum
    nEdges = eAccum.sum

    logger.info(s"edges: $nEdges")
    logger.info(s"vertices: $nVertices")

    g.mapPartitions { iter =>
      iter.map {
        case (vId: Long, _) =>

          (vId, Array(vId))
      }
    }.partitionBy(new HashPartitioner(config.rddPartitions)).cache()
  }

  def doFirsStepOfRandomWalk(paths: RDD[(Long, Array[Long])], nextDouble: () =>
    Double = Random.nextDouble): RDD[(Long, (Array[Long], Array[(Long, Double)], Long, Int))] = {
    val walkLength = context.broadcast(config.walkLength)
    paths.mapPartitions { iter =>
      val stepCounter = 0
      iter.map { case ((src: Long, path: Array[Long])) =>
        val neighbors = GraphMap.getNeighbors(path.head)
        if (neighbors != null && neighbors.length > 0) {
          val (nextStep, _) = RandomSample(nextDouble).sample(neighbors)
          (src, (path ++ Array(nextStep), GraphMap.getNeighbors(src), src, stepCounter))
        } else {
          // It's a deadend.
          (src, (path, Array.empty[(Long, Double)], src, walkLength.value))
        }
      }
    }
  }

  def randomWalk(initPaths: RDD[(Long, Array[Long])], nextDouble: () => Double = Random
    .nextDouble)
  : RDD[Array[Long]] = {
    val bcP = context.broadcast(config.p)
    val bcQ = context.broadcast(config.q)
    val walkLength = context.broadcast(config.walkLength)
    val numberOfWalks = context.broadcast(config.numWalks)
    val numPartitions = context.broadcast(config.rddPartitions)
    // initialize the first step of the random walk
    var totalPaths: RDD[Array[Long]] = context.emptyRDD[Array[Long]]
    for (_ <- 0 until numberOfWalks.value) {
      var pathsPieces: RDD[(Long, (Array[Long], Int))] = context.emptyRDD
      var newPaths: RDD[(Long, (Array[Long], Array[(Long, Double)], Long, Int))] =
        doFirsStepOfRandomWalk(initPaths, nextDouble)
      while (!newPaths.isEmpty()) {
        newPaths = newPaths.mapPartitions { iter =>
          iter.map { case (src: Long, (steps: Array[Long], prevNeighbors: Array[(Long, Double)],
          origin: Long, numSteps: Int)) =>
            var path = steps
            var stepCounter = numSteps
            val rSample = RandomSample(nextDouble)
            var pNeighbors = prevNeighbors
            if (steps.length > 1) {
              breakable {
                while (stepCounter < walkLength.value) {
                  val curr = path.last
                  val currNeighbors = GraphMap.getNeighbors(curr)
                  val prev = path(path.length - 2)
                  if (path.length > 2) { // If the walker is continuing on the local partition.
                    pNeighbors = GraphMap.getNeighbors(prev)
                  }
                  if (currNeighbors != null) {
                    if (currNeighbors.length > 0) {
                      stepCounter += 1
                      val (nextStep, _) = rSample.secondOrderSample(bcP.value, bcQ
                        .value, prev, pNeighbors, currNeighbors)
                      path = path ++ Array(nextStep)
                    } else {
                      stepCounter = walkLength.value
                      // This walker has reached a deadend. Needs to stop.
                    }
                  } else {
                    // The walker has reached to the edge of the partition. Needs a ride to
                    // another
                    // partition.
                    break
                  }
                }
              }
            }

            (src, (path, pNeighbors, origin, stepCounter))
          }
        }.cache()

        pathsPieces = pathsPieces.union(newPaths.map { case (_, (steps,
        _, origin,
        stepCounter)) =>
          if (stepCounter == walkLength.value)
            (origin, (steps, stepCounter))
          else
            (origin, (steps.slice(0, steps.length - 2), stepCounter))
        }).cache()
        newPaths = newPaths.filter(_._2._4 < walkLength.value).map { case (_, (steps,
        prevNeighbors, origin,
        stepCounter)) =>
          (steps.last, (steps.slice(steps.length - 2, steps.length), prevNeighbors, origin,
            stepCounter))
        }.partitionBy(new HashPartitioner(numPartitions.value))
      }

      totalPaths = totalPaths.union(pathsPieces.groupByKey(config.rddPartitions).mapValues { iter =>
        iter.toList.sortBy(_
          ._2).map(_._1)
      }.reduceByKey(_ ++ _).flatMap { case (_, steps) => steps })
        .persist(StorageLevel.MEMORY_AND_DISK)

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
