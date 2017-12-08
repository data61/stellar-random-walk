package au.csiro.data61.randomwalk.random

import au.csiro.data61.randomwalk.common.Params
import au.csiro.data61.randomwalk.{RandomSample, RandomWalk}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{HashPartitioner, SparkContext}

import scala.util.control.Breaks._
import scala.util.{Random, Try}

case class UniformRandomWalk(context: SparkContext,
                             config: Params) extends RandomWalk[(Int, Array[Int])] with
  Serializable {

  val partitioner = new HashPartitioner(config.rddPartitions)
  var routingTable = context.emptyRDD[Int]

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

  def doFirsStepOfRandomWalk(paths: RDD[(Int, Array[Int])], nextFloat: () =>
    Float = Random.nextFloat): RDD[(Int, (Array[Int], Array[(Int, Float)], Int,
    Int))] = {
    val walkLength = context.broadcast(config.walkLength)
    paths.mapPartitions({ iter =>
      val zeroStep = 0
      iter.map { case (src: Int, path: Array[Int]) =>
        val neighbors = RandomGraphMap.getNeighbors(path.head)
        if (neighbors != null && neighbors.length > 0) {
          val (nextStep, _) = RandomSample(nextFloat).sample(neighbors)
          (src, (path ++ Array(nextStep), RandomGraphMap.getNeighbors(src), src, zeroStep))
        } else {
          // It's a deadend.
          (src, (path, Array.empty[(Int, Float)], src, walkLength.value))
        }
      }
    }, preservesPartitioning = true
    )
  }

  def randomWalk(initPaths: RDD[(Int, Array[Int])], nextFloat: () => Float = Random
    .nextFloat)
  : RDD[List[Int]] = {
    val bcP = context.broadcast(config.p)
    val bcQ = context.broadcast(config.q)
    val walkLength = context.broadcast(config.walkLength)
    val numberOfWalks = context.broadcast(config.numWalks)
    var totalPaths: RDD[List[Int]] = context.emptyRDD[List[Int]]

    for (_ <- 0 until numberOfWalks.value) {
      //      val pathsPieces: mutable.ListBuffer[RDD[(Int, (Array[Int], Int))]] = ListBuffer
      // .empty
      var pathsPieces: RDD[(Int, (Array[Int], Int))] = context.emptyRDD[(Int, (Array[Int],
        Int))].partitionBy(partitioner)
      var unfinishedWalkers: RDD[(Int, (Array[Int], Array[(Int, Float)], Int, Int)
        )] = doFirsStepOfRandomWalk(initPaths, nextFloat)
      var prevUnfinished: RDD[(Int, (Array[Int], Array[(Int, Float)], Int, Int))] =
        context
          .emptyRDD[(Int, (Array[Int], Array[(Int, Float)], Int, Int))]
      var prevPieces = context.emptyRDD[(Int, (Array[Int], Int))]
      var remainingWalkers = Int.MaxValue

      val acc = context.longAccumulator("Error finder")
      val acc2 = context.longAccumulator("Error finder")
      do {
        val pieces = unfinishedWalkers.mapPartitions({ iter =>
          iter.map {
            case (_, (steps,
            _, origin,
            stepCounter)) =>
              if (stepCounter == walkLength.value)
                (origin, (steps, stepCounter))
              else
                (origin, (steps.slice(0, steps.length - 2), stepCounter))
          }
        }, preservesPartitioning = false)

        //        pieces.count()
        //        pathsPieces.append(pieces)

        prevPieces = pathsPieces
        pathsPieces = context.union(pathsPieces, pieces).persist(StorageLevel.MEMORY_AND_DISK)
        pathsPieces.count()
        prevPieces.unpersist(blocking = false)
        prevUnfinished = unfinishedWalkers
        unfinishedWalkers = transferWalkersToTheirPartitions(routingTable,
          prepareWalkersToTransfer(filterUnfinishedWalkers(unfinishedWalkers, walkLength)))
        val oldCount = remainingWalkers
        remainingWalkers = unfinishedWalkers.count().toInt
        prevUnfinished.unpersist(blocking = false)
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
          iter.map { case (src: Int, (steps: Array[Int], prevNeighbors: Array[(Int,
            Float)], origin: Int, numSteps: Int)) =>
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

            (src, (path, pNeighbors, origin, stepCounter))
          }
        }
          , preservesPartitioning = true
        ).persist(StorageLevel.MEMORY_AND_DISK)
      }
      while (remainingWalkers != 0)

      //      val allPieces = context.union(pathsPieces).persist(StorageLevel.MEMORY_AND_DISK)
      //      println(s"Total created path pieces: ${allPieces.count()}")
      //      pathsPieces.foreach(piece => piece.unpersist(blocking = false))

      totalPaths = totalPaths.union(sortPathPieces(pathsPieces)).persist(StorageLevel
        .MEMORY_AND_DISK)
      //      totalPaths = totalPaths.union(sortPathPieces(context.union(pathsPieces)))
      //        .persist(StorageLevel.MEMORY_AND_DISK)
      totalPaths.count()

    }

    totalPaths
  }

  def sortPathPieces(pathsPieces: RDD[(Int, (Array[Int], Int))]) = {
    pathsPieces.groupByKey(config.rddPartitions).mapPartitions({
      iter =>
        iter.map {
          case (_, it) =>
            it.toList.sortBy(_._2).flatMap(_._1)
        }
    }, preservesPartitioning = false)
  }


  def transferWalkersToTheirPartitions(routingTable: RDD[Int], walkers: RDD[(Int,
    (Array[Int], Array[(Int, Float)], Int, Int))]) = {
    routingTable.zipPartitions(walkers.partitionBy(partitioner)) {
      (_, iter2) =>
        iter2
    }
  }

  def filterUnfinishedWalkers(walkers: RDD[(Int, (Array[Int], Array[(Int, Float)],
    Int, Int))], walkLength: Broadcast[Int]) = {
    walkers.filter(_._2._4 < walkLength.value)
  }

  def prepareWalkersToTransfer(walkers: RDD[(Int, (Array[Int], Array[(Int, Float)],
    Int, Int))]) = {
    walkers.mapPartitions({
      iter =>
        iter.map {
          case (_, (steps, prevNeighbors, origin, stepCounter)) =>
            (steps.last, (steps.slice(steps.length - 2, steps.length), prevNeighbors, origin,
              stepCounter))
        }
    }, preservesPartitioning = false)

  }

  def mergeNewPaths(paths: RDD[(Int, (Array[Int], Int))], newPaths: RDD[(Int, (Array[Int],
    Array[(Int, Float)], Int, Int))], walkLength: Broadcast[Int]) = {
    paths.union(newPaths.mapPartitions({
      iter =>
        iter.map {
          case (_, (steps,
          _, origin,
          stepCounter)) =>
            if (stepCounter == walkLength.value)
              (origin, (steps, stepCounter))
            else
              (origin, (steps.slice(0, steps.length - 2), stepCounter))
        }
    }, preservesPartitioning = false))
  }

}
