package au.csiro.data61.randomwalk.vertexcut

import au.csiro.data61.randomwalk.common.Params
import au.csiro.data61.randomwalk.{RandomSample, RandomWalk}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.util.control.Breaks.{break, breakable}
import scala.util.{Random, Try}

case class VCutRandomWalk(context: SparkContext,
                          config: Params) extends RandomWalk {

  def loadGraph(): RDD[(Int, (Array[Int]))] = {
    val bcDirected = context.broadcast(config.directed)
    val bcWeighted = context.broadcast(config.weighted) // is weighted?
    val bcRddPartitions = context.broadcast(config.rddPartitions)
    val bcPartitioned = context.broadcast(config.partitioned)

    val edgePartitions: RDD[(Int, (Array[(Int, Int, Float)], Int))] = context.textFile(config
      .input, minPartitions = config.rddPartitions).flatMap { triplet =>
      val parts = triplet.split("\\s+")

      val pId: Int = bcPartitioned.value && parts.length > 2 match {
        case true => Try(parts(2).toInt).getOrElse(Random.nextInt(bcRddPartitions.value))
        case false => Random.nextInt(bcRddPartitions.value)
      }

      // if the weights are not specified it sets it to 1.0
      val weight = bcWeighted.value && parts.length > 3 match {
        case true => Try(parts.last.toFloat).getOrElse(1.0f)
        case false => 1.0f
      }

      val (src, dst) = (parts.head.toInt, parts(1).toInt)
      val srcTuple = (src, (Array((dst, pId, weight)), pId))
      if (bcDirected.value) {
        Array(srcTuple, (dst, (Array.empty[(Int, Int, Float)], pId)))
      } else {
        Array(srcTuple, (dst, (Array((src, pId, weight)), pId)))
      }
    }.partitionBy(partitioner).persist(StorageLevel.MEMORY_AND_DISK)

    val vertexPartitions = edgePartitions.mapPartitions({ iter =>
      iter.map { case (src, (_, pId)) =>
        (src, pId)
      }
    }, preservesPartitioning = true).cache()

    val vertexNeighbors = edgePartitions.reduceByKey((x, y) => (x._1 ++ y._1, x._2)).cache

    val g: RDD[(Int, (Int, Array[(Int, Int, Float)]))] =
      vertexPartitions.join(vertexNeighbors).map {
        case (v, (pId, (neighbors, _))) => (pId, (v, neighbors))
      }.partitionBy(partitioner)

    routingTable = buildRoutingTable(g).persist(StorageLevel.MEMORY_ONLY)
    routingTable.count()

    val vAccum = context.longAccumulator("vertices")
    val eAccum = context.longAccumulator("edges")

    val rAcc = context.collectionAccumulator[Int]("replicas")
    val lAcc = context.collectionAccumulator[Int]("links")

    vertexNeighbors.foreachPartition { iter =>
      val (r, e) = PartitionedGraphMap.getGraphStatsOnlyOnce
      if (r != 0) {
        rAcc.add(r)
        lAcc.add(e)
      }
      iter.foreach {
        case (_, (neighbors: Array[(Int, Int, Float)], _)) =>
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

    val walkers = vertexNeighbors.map {
      case (vId: Int, (_, pId: Int)) =>
        (pId, Array(vId))
    }

    initWalkersToTheirPartitions(routingTable, walkers).persist(StorageLevel.MEMORY_AND_DISK)
  }

  def randomWalk(initPaths: RDD[(Int, Array[Int])], nextFloat: () => Float = Random
    .nextFloat): RDD[Array[Int]] = {
    val bcP = context.broadcast(config.p)
    val bcQ = context.broadcast(config.q)
    val walkLength = context.broadcast(config.walkLength)
    var totalPaths: RDD[Array[Int]] = context.emptyRDD[Array[Int]]

    for (_ <- 0 until config.numWalks) {
      var unfinishedWalkers: RDD[(Int, (Array[Int], Array[(Int, Float)], Boolean))] = initFirstStep(
        initPaths, nextFloat)
      var pathsPieces: RDD[Array[Int]] = context.emptyRDD[Array[Int]].repartition(config
        .rddPartitions)
      //      var prevPieces: RDD[Array[Int]] = context.emptyRDD[Array[Int]]
      //      var prevUnfinished: RDD[(Int, (Array[Int], Array[(Int, Float)], Int))] =
      //        context.emptyRDD[(Int, (Array[Int], Array[(Int, Float)], Int))]
      var remainingWalkers = Int.MaxValue

      val acc = context.longAccumulator("Error finder")
      val acc2 = context.longAccumulator("Error finder")
      do {

        //        prevPieces = pathsPieces
        //        pathsPieces = pathsPieces.union(filterCompletedPaths(unfinishedWalkers))
        //          .persist(StorageLevel.MEMORY_AND_DISK)
        //        pathsPieces.count()
        //        //        prevUnfinished = unfinishedWalkers
        //        unfinishedWalkers = transferWalkersToTheirPartitions(routingTable,
        //          prepareWalkersToTransfer(filterUnfinishedWalkers(unfinishedWalkers)))
        //        unfinishedWalkers = transferWalkersToTheirPartitions(routingTable,
        //          prepareWalkersToTransfer(unfinishedWalkers))
        //        val oldCount = remainingWalkers
        //        remainingWalkers = unfinishedWalkers.count().toInt
        //        prevUnfinished.unpersist(blocking = false)
        //        prevPieces.unpersist(blocking = false)
        //        if (remainingWalkers > oldCount) {
        //          logger.warn(s"Inconsistent state: number of unfinished walkers was increased!")
        //          println(s"Inconsistent state: number of unfinished walkers was increased!")
        //        }
        //        println(s"Unfinished Walkers: $remainingWalkers")
        //        if (!acc.isZero || !acc2.isZero) {
        //          println(s"Wrong Transports: ${acc.sum}")
        //          println(s"Zero Neighbors: ${acc2.sum}")
        //          acc.reset()
        //          acc2.reset()
        //        }

        //        prevUnfinished = unfinishedWalkers
        unfinishedWalkers = transferWalkersToTheirPartitions(routingTable,
          prepareWalkersToTransfer(unfinishedWalkers))

        unfinishedWalkers = unfinishedWalkers.mapPartitions({ iter =>
          iter.map { case (pId, (steps: Array[Int], prevNeighbors: Array[(Int, Float)],
          completed: Boolean)) =>
            var path = steps
            var isCompleted = completed
            val rSample = RandomSample(nextFloat)
            var pNeighbors: Array[(Int, Float)] = prevNeighbors
            breakable {
              while (!isCompleted && path.length != walkLength.value + 2) {
                val currNeighbors = PartitionedGraphMap.getNeighbors(path.last)
                val prev = path(path.length - 2)
                if (path.length > steps.length) { // If the walker is continuing on the local
                  // partition.
                  pNeighbors = PartitionedGraphMap.getNeighbors(prev)
                }
                if (currNeighbors != null) {
                  if (currNeighbors.length > 0) {
                    val (nextStep, _) = rSample.secondOrderSample(bcP.value.toInt, bcQ.value
                      .toInt, prev, pNeighbors, currNeighbors)
                    path = path ++ Array(nextStep)
                  } else {
                    isCompleted = true
                    acc2.add(1)
                    break
                    // This walker has reached a deadend. Needs to stop.
                  }
                } else {
                  if (path.length == steps.length) {
                    acc.add(1)
                  }
                  // The walker has reached to the edge of the partition. Needs a ride to
                  // another
                  // partition.
                  break
                }
              }
            }
            if (path.length == walkLength.value + 2)
              isCompleted = true

            (pId, (path, pNeighbors, isCompleted))
          }
        }
          , preservesPartitioning = true
        ).persist(StorageLevel.MEMORY_AND_DISK)

        pathsPieces = pathsPieces.union(filterCompletedPaths(unfinishedWalkers))
          .persist(StorageLevel.MEMORY_AND_DISK)
        pathsPieces.count()

        unfinishedWalkers = filterUnfinishedWalkers(unfinishedWalkers)

        val oldCount = remainingWalkers
        //        remainingWalkers = filterUnfinishedWalkers(unfinishedWalkers).count().toInt
        remainingWalkers = unfinishedWalkers.count().toInt

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
      }
      while (remainingWalkers != 0)

//      val paths = filterCompletedPaths(unfinishedWalkers).persist(StorageLevel
//        .MEMORY_AND_DISK)
      val pCount = pathsPieces.count()
      if (pCount != nVertices) {
        println(s"Inconsistent number of paths: nPaths=[${pCount}] != vertices[$nVertices]")
      }
      totalPaths = totalPaths.union(pathsPieces).persist(StorageLevel
        .MEMORY_AND_DISK)

      totalPaths.count()

    }

    totalPaths
  }

  def filterUnfinishedWalkers(walkers: RDD[(Int, (Array[Int], Array[(Int, Float)], Boolean))]) = {
    walkers.filter(!_._2._3)
  }

  def initWalkersToTheirPartitions(routingTable: RDD[Int], walkers: RDD[(Int, Array[Int])]) = {
    routingTable.zipPartitions(walkers.partitionBy(partitioner)) {
      (_, iter2) =>
        iter2
    }
  }

  def buildRoutingTable(graph: RDD[(Int, (Int, Array[(Int, Int, Float)]))]): RDD[Int] = {

    graph.mapPartitionsWithIndex({ (id: Int, iter: Iterator[(Int, (Int, Array[(Int, Int,
      Float)]))]) =>
      iter.foreach { case (_, (vId, neighbors)) =>
        PartitionedGraphMap.addVertex(vId, neighbors)
        id
      }
      Iterator.empty
    }, preservesPartitioning = true
    )

  }

  def initFirstStep(paths: RDD[(Int, Array[Int])], nextFloat: () =>
    Float = Random.nextFloat): RDD[(Int, (Array[Int], Array[(Int, Float)], Boolean))] = {
    paths.mapPartitions({ iter =>
      iter.map { case (pId, path: Array[Int]) =>
        val neighbors = PartitionedGraphMap.getNeighbors(path.head)
        if (neighbors != null && neighbors.length > 0) {
          val (nextStep, _) = RandomSample(nextFloat).sample(neighbors)
          (pId, (path ++ Array(nextStep), neighbors, false))
        } else {
          // It's a deaend.
          (pId, (path, Array.empty[(Int, Float)], true))
        }
      }
    }, preservesPartitioning = true
    )
  }

  def prepareWalkersToTransfer(walkers: RDD[(Int, (Array[Int], Array[(Int, Float)], Boolean))]) = {
    walkers.mapPartitions({
      iter =>
        iter.map {
          case (_, (steps, prevNeighbors, completed)) =>
            val pId = PartitionedGraphMap.getPartition(steps.last) match {
              case Some(pId) => pId
              case None => -1 // Must exists!
            }
            (pId, (steps, prevNeighbors, completed))
        }
    }, preservesPartitioning = false)

  }

}
