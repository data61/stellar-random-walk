package au.csiro.data61.randomwalk.efficient

import au.csiro.data61.Main
import com.navercorp.common.Property
import org.apache.log4j.LogManager
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{HashPartitioner, SparkContext}

import scala.util.control.Breaks._
import scala.util.{Random, Try}

case class RandomWalk(context: SparkContext,
                      config: Main.Params) extends Serializable {

  lazy val logger = LogManager.getLogger("rwLogger")
  var nVertices: Long = 0
  var nEdges: Long = 0
  val partitioner = new HashPartitioner(config.rddPartitions)
  var routingTable = context.emptyRDD[Int]

  def loadGraph(): RDD[(Int, (Long, Array[Long]))] = {
    val bcDirected = context.broadcast(config.directed)
    val bcWeighted = context.broadcast(config.weighted) // is weighted?
    val bcRddPartitions = context.broadcast(config.rddPartitions)

    val edgePartitions: RDD[(Long, (Array[(Long, Int, Double)], Int))] = context.textFile(config
      .input, minPartitions
      = config
      .rddPartitions).flatMap { triplet =>
      val parts = triplet.split("\\s+")

      val pId: Int = parts.length > 2 match {
        case true => Try(parts(2).toInt).getOrElse(Random.nextInt(bcRddPartitions.value))
        case false => Random.nextInt(bcRddPartitions.value)
      }

      // if the weights are not specified it sets it to 1.0
      val weight = bcWeighted.value && parts.length > 3 match {
        case true => Try(parts.last.toDouble).getOrElse(1.0)
        case false => 1.0
      }

      val (src, dst) = (parts.head.toLong, parts(1).toLong)
      val srcTuple = (src, (Array((dst, pId, weight)), pId))
      if (bcDirected.value) {
        Array(srcTuple, (dst, (Array.empty[(Long, Int, Double)], pId)))
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

    val g: RDD[(Int, (Long, Array[(Long, Int, Double)]))] =
      vertexPartitions.join(vertexNeighbors).map {
        case (v, (pId, (neighbors, _))) => (pId, (v, neighbors))
      }.partitionBy(partitioner).persist(StorageLevel.MEMORY_AND_DISK)

    routingTable = buildRoutingTable(g).persist(StorageLevel.MEMORY_ONLY)
    routingTable.count()

    val vAccum = context.longAccumulator("vertices")
    val eAccum = context.longAccumulator("edges")

    vertexNeighbors.foreachPartition { iter =>
      iter.foreach {
        case (_, (neighbors: Array[(Long, Int, Double)], _)) =>
          vAccum.add(1)
          eAccum.add(neighbors.length)
      }
    }
    nVertices = vAccum.sum
    nEdges = eAccum.sum

    logger.info(s"edges: $nEdges")
    logger.info(s"vertices: $nVertices")
    println(s"edges: $nEdges")
    println(s"vertices: $nVertices")

    val walkers = vertexNeighbors.map {
      case (vId: Long, (_, pId: Int)) =>
        (pId, (vId, Array(vId)))
    }.persist(StorageLevel.MEMORY_AND_DISK)

    initWalkersToTheirPartitions(routingTable, walkers)
  }

  def initWalkersToTheirPartitions(routingTable: RDD[Int], walkers: RDD[(Int, (Long,
    Array[Long]))]) = {
    routingTable.zipPartitions(walkers.partitionBy(partitioner)) {
      (_, iter2) =>
        iter2
    }
  }

  def buildRoutingTable(graph: RDD[(Int, (Long, Array[(Long, Int, Double)]))]): RDD[Int] = {
    graph.mapPartitionsWithIndex({ (id: Int, iter: Iterator[(Int, (Long, Array[(Long, Int,
      Double)]))]) =>
      iter.foreach { case (_, (vId, neighbors)) =>
        GraphMap.addVertex(vId, neighbors)
        id
      }
      Iterator.empty
    }, preservesPartitioning = true
    )
  }

  //  def initRandomWalk(g: RDD[(Int, (Long, Array[(Long, Double)]))]) = {
  //    routingTable = buildRoutingTable(g).persist(StorageLevel.MEMORY_ONLY)
  //    routingTable.count()
  //
  //    val vAccum = context.longAccumulator("vertices")
  //    val eAccum = context.longAccumulator("edges")
  //
  //    g.foreachPartition { iter =>
  //      iter.foreach {
  //        case (vId: Long, (_, neighbors: Array[(Long, Double)])) =>
  //          vAccum.add(1)
  //          eAccum.add(neighbors.length)
  //      }
  //    }
  //    nVertices = vAccum.sum
  //    nEdges = eAccum.sum
  //
  //    logger.info(s"edges: $nEdges")
  //    logger.info(s"vertices: $nVertices")
  //    println(s"edges: $nEdges")
  //    println(s"vertices: $nVertices")
  //
  //    g.mapPartitions({ iter =>
  //      iter.map {
  //        case (vId: Long, _) =>
  //          (vId, Array(vId))
  //      }
  //    }, preservesPartitioning = true
  //    )
  //  }

  //  /**
  //    * Loads the graph and computes the probabilities to go from each vertex to its neighbors
  //    *
  //    * @return
  //    */
  //  def loadGraph(): RDD[(Long, Array[Long])] = {
  //    // the directed and weighted parameters are only used for building the graph object.
  //    // is directed? they will be shared among stages and executors
  //    val bcDirected = context.broadcast(config.directed)
  //    val bcWeighted = context.broadcast(config.weighted) // is weighted?
  //
  //    val g: RDD[(Long, Array[(Long, Double)])] = context.textFile(config.input, minPartitions
  //      = config
  //      .rddPartitions).flatMap { triplet =>
  //      val parts = triplet.split("\\s+")
  //      // if the weights are not specified it sets it to 1.0
  //
  //      val weight = bcWeighted.value && parts.length > 2 match {
  //        case true => Try(parts.last.toDouble).getOrElse(1.0)
  //        case false => 1.0
  //      }
  //
  //      val (src, dst) = (parts.head.toLong, parts(1).toLong)
  //      if (bcDirected.value) {
  //        Array((src, Array((dst, weight))), (dst, Array.empty[(Long, Double)]))
  //      } else {
  //        Array((src, Array((dst, weight))), (dst, Array((src, weight))))
  //      }
  //    }.
  //      reduceByKey(_ ++ _).
  //      partitionBy(partitioner).
  //      persist(StorageLevel.MEMORY_AND_DISK) // TODO: Apply a smart graph partition strategy
  //
  //    routingTable = buildRoutingTable(g).persist(StorageLevel.MEMORY_ONLY)
  //    routingTable.count()
  //
  //    val vAccum = context.longAccumulator("vertices")
  //    val eAccum = context.longAccumulator("edges")
  //
  //    g.foreachPartition { iter =>
  //      iter.foreach {
  //        case (vId: Long, (neighbors: Array[(Long, Double)])) =>
  //          vAccum.add(1)
  //          eAccum.add(neighbors.length)
  //      }
  //    }
  //    nVertices = vAccum.sum
  //    nEdges = eAccum.sum
  //
  //    logger.info(s"edges: $nEdges")
  //    logger.info(s"vertices: $nVertices")
  //    println(s"edges: $nEdges")
  //    println(s"vertices: $nVertices")
  //
  //    g.mapPartitions({ iter =>
  //      iter.map {
  //        case (vId: Long, _) =>
  //          (vId, Array(vId))
  //      }
  //    }, preservesPartitioning = true
  //    )
  //  }

  def doFirsStepOfRandomWalk(paths: RDD[(Int, (Long, Array[Long]))], nextDouble: () =>
    Double = Random.nextDouble): RDD[(Int, (Long, Array[Long], Array[(Long, Int, Double)], Long,
    Int))] = {
    val walkLength = context.broadcast(config.walkLength)
    paths.mapPartitions({ iter =>
      val zeroStep = 0
      iter.map { case (pId, (src: Long, path: Array[Long])) =>
        val neighbors = GraphMap.getNeighbors(path.head)
        if (neighbors != null && neighbors.length > 0) {
          val (nextStep, pId, _) = RandomSample(nextDouble).sample(neighbors)
          (pId, (src, path ++ Array(nextStep), GraphMap.getNeighbors(src), src, zeroStep))
        } else {
          // It's a deadend.
          (pId, (src, path, Array.empty[(Long, Int, Double)], src, walkLength.value))
        }
      }
    }, preservesPartitioning = true
    )
  }

  def randomWalk(initPaths: RDD[(Int, (Long, Array[Long]))], nextDouble: () => Double = Random
    .nextDouble)
  : RDD[List[Long]] = {
    val bcP = context.broadcast(config.p)
    val bcQ = context.broadcast(config.q)
    val walkLength = context.broadcast(config.walkLength)
    val numberOfWalks = context.broadcast(config.numWalks)
    var totalPaths: RDD[List[Long]] = context.emptyRDD[List[Long]]

    for (_ <- 0 until numberOfWalks.value) {
      //      val pathsPieces: mutable.ListBuffer[RDD[(Long, (Array[Long], Int))]] = ListBuffer
      // .empty
      var pathsPieces: RDD[(Long, (Array[Long], Int))] = context.emptyRDD[(Long, (Array[Long],
        Int))].partitionBy(partitioner)
      var unfinishedWalkers: RDD[(Int, (Long, Array[Long], Array[(Long, Int, Double)], Long, Int)
        )] = doFirsStepOfRandomWalk(initPaths, nextDouble)
      var prevUnfinished: RDD[(Int, (Long, Array[Long], Array[(Long, Int, Double)], Long, Int))] =
        context
          .emptyRDD[(Int, (Long, Array[Long], Array[(Long, Int, Double)], Long, Int))]
      var prevPieces = context.emptyRDD[(Long, (Array[Long], Int))]
      var remainingWalkers = Long.MaxValue

      val acc = context.longAccumulator("Error finder")
      val acc2 = context.longAccumulator("Error finder")
      do {
        val pieces = unfinishedWalkers.mapPartitions({ iter =>
          iter.map {
            case (_, (_, steps,
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
        remainingWalkers = unfinishedWalkers.count()
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
          iter.map { case (pId, (src: Long, steps: Array[Long], prevNeighbors: Array[(Long, Int,
            Double)], origin: Long, numSteps: Int)) =>
            var path = steps
            var stepCounter = numSteps
            val rSample = RandomSample(nextDouble)
            var pNeighbors = prevNeighbors
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
                    val (nextStep, _, _) = rSample.secondOrderSample(bcP.value, bcQ
                      .value, prev, pNeighbors, currNeighbors)
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

            (pId, (src, path, pNeighbors, origin, stepCounter))
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

  def sortPathPieces(pathsPieces: RDD[(Long, (Array[Long], Int))]) = {
    pathsPieces.groupByKey(config.rddPartitions).mapPartitions({
      iter =>
        iter.map {
          case (_, it) =>
            it.toList.sortBy(_._2).flatMap(_._1)
        }
    }, preservesPartitioning = false)
  }


  def transferWalkersToTheirPartitions(routingTable: RDD[Int], walkers: RDD[(Int, (Long,
    Array[Long], Array[(Long, Int, Double)], Long, Int))]) = {
    routingTable.zipPartitions(walkers.partitionBy(partitioner)) {
      (_, iter2) =>
        iter2
    }
  }

  def filterUnfinishedWalkers(walkers: RDD[(Int, (Long, Array[Long], Array[(Long, Int, Double)],
    Long, Int))], walkLength: Broadcast[Int]) = {
    walkers.filter(_._2._5 < walkLength.value)
  }

  def prepareWalkersToTransfer(walkers: RDD[(Int, (Long, Array[Long], Array[(Long, Int, Double)],
    Long, Int))]) = {
    walkers.mapPartitions({
      iter =>
        iter.map {
          case (_, (_, steps,
          prevNeighbors, origin,
          stepCounter)) =>
            val pId = GraphMap.getPartition(steps.last) match {
              case Some(pId) => pId
              case None => -1 // Must exists!
            }
            (pId, (steps.last, steps.slice(steps.length - 2, steps.length), prevNeighbors, origin,
              stepCounter))
        }
    }, preservesPartitioning = false)

  }

  def mergeNewPaths(paths: RDD[(Long, (Array[Long], Int))], newPaths: RDD[(Long, (Array[Long],
    Array[(Long, Int, Double)], Long, Int))], walkLength: Broadcast[Int]) = {
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

  def save(paths: RDD[List[Long]]) = {

    paths.map {
      case (path) =>
        val pathString = path.mkString("\t")
        s"$pathString"
    }.repartition(config.rddPartitions).saveAsTextFile(s"${
      config.output
    }" +
      s".${
        Property.pathSuffix
      }")
  }

}
