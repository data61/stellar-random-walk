package au.csiro.data61.randomwalk.random

import au.csiro.data61.randomwalk.common.Params
import au.csiro.data61.randomwalk.{RandomSample, RandomWalk}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.util.control.Breaks.{break, breakable}
import scala.util.{Random, Try}

case class UniformRandomWalk(context: SparkContext, config: Params) extends RandomWalk {

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

    val g: RDD[(Int, Array[(Int, Float)])] = context.textFile(config.input, minPartitions
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
    }.
      reduceByKey(_ ++ _).
      partitionBy(partitioner).
      persist(StorageLevel.MEMORY_AND_DISK)

    routingTable = buildRoutingTable(g).persist(StorageLevel.MEMORY_ONLY)
    routingTable.count()

    val vAccum = context.longAccumulator("vertices")
    val eAccum = context.longAccumulator("edges")

    val rAcc = context.collectionAccumulator[Int]("replicas")
    val lAcc = context.collectionAccumulator[Int]("links")

    g.foreachPartition { iter =>
      val (r, e) = RandomGraphMap.getGraphStatsOnlyOnce
      if (r != 0) {
        rAcc.add(r)
        lAcc.add(e)
      }
      iter.foreach {
        case (_, (neighbors: Array[(Int, Float)])) =>
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

  def randomWalk(initPaths: RDD[(Int, Array[Int])], nextFloat: () => Float = Random.nextFloat)
  : RDD[Array[Int]] = {
    val bcP = context.broadcast(config.p)
    val bcQ = context.broadcast(config.q)
    val walkLength = context.broadcast(config.walkLength)
    val numberOfWalks = context.broadcast(config.numWalks)
    var totalPaths: RDD[Array[Int]] = context.emptyRDD[Array[Int]]

    for (_ <- 0 until numberOfWalks.value) {
      //      val pathsPieces: mutable.ListBuffer[RDD[(Int, (Array[Int], Int))]] = ListBuffer
      // .empty
      var unfinishedWalkers: RDD[(Int, (Array[Int], Array[(Int, Float)], Int))] =
      initFirstStep(initPaths, nextFloat)
      var pathsPieces: RDD[Array[Int]] = context.emptyRDD[Array[Int]].repartition(config
        .rddPartitions)
      var prevPieces: RDD[Array[Int]] = null
      var prevUnfinished: RDD[(Int, (Array[Int], Array[(Int, Float)], Int))] =
        context.emptyRDD[(Int, (Array[Int], Array[(Int, Float)], Int))]
      var remainingWalkers = Int.MaxValue

      val acc = context.longAccumulator("Error finder")
      val acc2 = context.longAccumulator("Error finder")
      do {

        prevPieces = pathsPieces
        pathsPieces = context.union(filterCompletedPaths(unfinishedWalkers, walkLength),
          pathsPieces)
          .persist(StorageLevel.MEMORY_AND_DISK)
        pathsPieces.count()
        prevUnfinished = unfinishedWalkers
        unfinishedWalkers = transferWalkersToTheirPartitions(routingTable,
          prepareWalkersToTransfer(filterUnfinishedWalkers(unfinishedWalkers, walkLength)))
        val oldCount = remainingWalkers
        remainingWalkers = unfinishedWalkers.count().toInt
        prevUnfinished.unpersist(blocking = false)
        prevPieces.unpersist(blocking = false)
        if (remainingWalkers > oldCount) {
          logger.warn(s"Inconsistent state: number of unfinished walkers was increased!")
          println(s"Inconsistent state: number of unfinished walkers was increased!")
        }
        println(s"Unfinished Walkers: $remainingWalkers")
        if (!acc.isZero || !acc2.isZero) {
          println(s"Wrong Transports: ${acc.sum}")
          println(s"Zero Neighbors: ${acc2.sum}")
          acc.reset()
          acc2.reset()
        }

        unfinishedWalkers = unfinishedWalkers.mapPartitions({ iter =>
          iter.map { case (pId, (steps: Array[Int], prevNeighbors: Array[(Int, Float)],
          numSteps: Int)) =>
            var path = steps
            var stepCounter = numSteps
            val rSample = RandomSample(nextFloat)
            var pNeighbors = prevNeighbors
            breakable {
              while (stepCounter < walkLength.value) {
                val curr = path.last
                val currNeighbors = RandomGraphMap.getNeighbors(curr)
                val prev = path(path.length - 2)
                if (path.length > 2) { // If the walker is continuing on the local partition.
                  pNeighbors = RandomGraphMap.getNeighbors(prev)
                }
                if (currNeighbors != null) {
                  if (currNeighbors.length > 0) {
                    stepCounter += 1
                    val (nextStep, _) = rSample.secondOrderSample(bcP.value.toInt, bcQ.value
                      .toInt, prev, pNeighbors, currNeighbors)
                    path = path ++ Array(nextStep)
                  } else {
                    stepCounter = walkLength.value
                    acc2.add(1)
                    break
                    // This walker has reached a deadend. Needs to stop.
                  }
                } else {
                  if (path.length == 2) {
                    acc.add(1)
                  }
                  // The walker has reached to the edge of the partition. Needs a ride to
                  // another
                  // partition.
                  break
                }
              }
            }

            (pId, (path, pNeighbors, stepCounter))
          }
        }
          , preservesPartitioning = true
        ).persist(StorageLevel.MEMORY_AND_DISK)
      }
      while (remainingWalkers != 0)

      totalPaths = totalPaths.union(pathsPieces).persist(StorageLevel
        .MEMORY_AND_DISK)

      totalPaths.count()

    }

    totalPaths
  }

  def buildRoutingTable(graph: RDD[(Int, Array[(Int, Float)])]): RDD[Int] = {

    graph.mapPartitionsWithIndex({ (id: Int, iter: Iterator[(Int, Array[(Int, Float)])]) =>
      iter.foreach { case (vId, neighbors) =>
        RandomGraphMap.addVertex(vId, neighbors)
        id
      }
      Iterator.empty
    }, preservesPartitioning = true
    )

  }

  def filterUnfinishedWalkers(walkers: RDD[(Int, (Array[Int], Array[(Int, Float)], Int))],
                              walkLength: Broadcast[Int]) = {
    walkers.filter(_._2._3 < walkLength.value)
  }

  def initFirstStep(paths: RDD[(Int, Array[Int])], nextFloat: () =>
    Float = Random.nextFloat): RDD[(Int, (Array[Int], Array[(Int, Float)], Int))] = {
    val walkLength = context.broadcast(config.walkLength)
    paths.mapPartitions({ iter =>
      val zeroStep = 0
      iter.map { case (src: Int, path: Array[Int]) =>
        val neighbors = RandomGraphMap.getNeighbors(path.head)
        if (neighbors != null && neighbors.length > 0) {
          val (nextStep, _) = RandomSample(nextFloat).sample(neighbors)
          (src, (path ++ Array(nextStep), RandomGraphMap.getNeighbors(src), zeroStep))
        } else {
          // It's a deadend.
          (src, (path, Array.empty[(Int, Float)], walkLength.value))
        }
      }
    }, preservesPartitioning = true
    )
  }

  def prepareWalkersToTransfer(walkers: RDD[(Int, (Array[Int], Array[(Int, Float)], Int))]) = {
    walkers.mapPartitions({
      iter =>
        iter.map {
          case (_, (steps, prevNeighbors, stepCounter)) => (steps.last, (steps, prevNeighbors,
            stepCounter))
        }
    }, preservesPartitioning = false)

  }

}
