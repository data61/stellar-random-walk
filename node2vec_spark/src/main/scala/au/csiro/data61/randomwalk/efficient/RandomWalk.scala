package au.csiro.data61.randomwalk.efficient

import au.csiro.data61.Main
import com.navercorp.common.Property
import org.apache.log4j.LogManager
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{HashPartitioner, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks._
import scala.util.{Random, Try}

case class RandomWalk(context: SparkContext,
                      config: Main.Params) extends Serializable {

  lazy val logger = LogManager.getLogger("rwLogger")
  var nVertices: Long = 0
  var nEdges: Long = 0
  val partitioner = new HashPartitioner(config.rddPartitions)
  var routingTable = context.emptyRDD[Int]

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
        Array((src, Array((dst, weight))), (dst, Array.empty[(Long, Double)]))
      } else {
        Array((src, Array((dst, weight))), (dst, Array((src, weight))))
      }
    }.
      reduceByKey(_ ++ _).
      partitionBy(partitioner).
      persist(StorageLevel.MEMORY_AND_DISK) // TODO: Apply a smart graph partition strategy


    val vAccum = context.longAccumulator("vertices")
    val eAccum = context.longAccumulator("edges")
    val vPaths = g.mapPartitions({ iter =>
      iter.map {
        case (vId: Long, (neighbors: Array[(Long, Double)])) =>
          GraphMap.addVertex(vId, neighbors)
          vAccum.add(1)
          eAccum.add(neighbors.length)
          (vId, Array(vId))
      }
    }, preservesPartitioning = true
    ).cache()

    routingTable = buildRoutingTable(vPaths).persist(StorageLevel.MEMORY_AND_DISK)

    vPaths.count()
    nVertices = vAccum.sum
    nEdges = eAccum.sum

    logger.info(s"edges: $nEdges")
    logger.info(s"vertices: $nVertices")
    println(s"edges: $nEdges")
    println(s"vertices: $nVertices")

    vPaths
  }

  def buildRoutingTable(graph: RDD[(Long, Array[Long])]): RDD[Int] = {
    graph.mapPartitionsWithIndex({ (id: Int, iter: Iterator[(Long, _)]) =>
      iter.map {
        case (_, _) =>
          id
      }
      Iterator.empty
    }
      , preservesPartitioning = true
    )
  }

  def doFirsStepOfRandomWalk(paths: RDD[(Long, Array[Long])], nextDouble: () =>
    Double = Random.nextDouble): RDD[(Long, (Array[Long], Array[(Long, Double)], Long, Int))] = {
    val walkLength = context.broadcast(config.walkLength)
    paths.mapPartitions({ iter =>
      val zeroStep = 0
      iter.map { case ((src: Long, path: Array[Long])) =>
        val neighbors = GraphMap.getNeighbors(path.head)
        if (neighbors != null && neighbors.length > 0) {
          val (nextStep, _) = RandomSample(nextDouble).sample(neighbors)
          (src, (path ++ Array(nextStep), GraphMap.getNeighbors(src), src, zeroStep))
        } else {
          // It's a deadend.
          (src, (path, Array.empty[(Long, Double)], src, walkLength.value))
        }
      }
    }, preservesPartitioning = true
    )
  }

  def randomWalk(initPaths: RDD[(Long, Array[Long])], nextDouble: () => Double = Random
    .nextDouble)
  : RDD[List[Long]] = {
    val bcP = context.broadcast(config.p)
    val bcQ = context.broadcast(config.q)
    val walkLength = context.broadcast(config.walkLength)
    val numberOfWalks = context.broadcast(config.numWalks)
    var totalPaths: RDD[List[Long]] = context.emptyRDD[List[Long]]

    for (_ <- 0 until numberOfWalks.value) {
      val pathsPieces: mutable.ListBuffer[RDD[(Long, (Array[Long], Int))]] = ListBuffer.empty
      var newPaths: RDD[(Long, (Array[Long], Array[(Long, Double)], Long, Int))] =
        doFirsStepOfRandomWalk(initPaths, nextDouble)
      var remainingWalkers = newPaths.count()

      val acc = context.longAccumulator("Error finder")
      val mAcc = context.collectionAccumulator[String]("Trace")
      do {
        val pieces = newPaths.mapPartitions({ iter =>
          iter.map {
            case (_, (steps,
            _, origin,
            stepCounter)) =>
              if (stepCounter == walkLength.value)
                (origin, (steps, stepCounter))
              else
                (origin, (steps.slice(0, steps.length - 2), stepCounter))
          }
        }, preservesPartitioning = false).persist(StorageLevel.MEMORY_AND_DISK)

        pieces.count()
        pathsPieces.append(pieces)

        val remaining = transferWalkersToTheirPartitions(routingTable, prepareWalkersToTransfer
        (filterUnfinishedWalkers(newPaths, walkLength))).cache()
        remainingWalkers = remaining.count()
        newPaths = remaining
        println(s"Remaining Walkers: $remainingWalkers")
        if (!acc.isZero) {
          println(s"Wrong Transports: ${acc.sum}")
          println
          (s"Trace: ${mAcc.value.toArray.mkString("\n")}")
          acc.reset()
          mAcc.reset()
        }

        //      while (remainingWalkers != 0) {
        newPaths = newPaths.mapPartitions({ iter =>
          iter.map { case (src: Long, (steps: Array[Long], prevNeighbors: Array[(Long, Double)],
          origin: Long, numSteps: Int)) =>
            var path = steps
            var stepCounter = numSteps
            val rSample = RandomSample(nextDouble)
            var pNeighbors = prevNeighbors
            //            if (path.length > 1) {
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
                    break
                    // This walker has reached a deadend. Needs to stop.
                  }
                } else {
                  if (path.length == 2) {
                    acc.add(1)
                    mAcc.add(s"Path=${path.mkString("->")}\t head=$src\t numSteps=$stepCounter")
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
        ).cache()
      }
      while (remainingWalkers != 0)

      val allPieces = context.union(pathsPieces).persist(StorageLevel.MEMORY_AND_DISK)
      println(s"Total created path pieces: ${allPieces.count()}")
      totalPaths = totalPaths.union(sortPathPieces(allPieces).persist(StorageLevel
        .MEMORY_AND_DISK))
        .persist(StorageLevel.MEMORY_AND_DISK)
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


  def transferWalkersToTheirPartitions(routingTable: RDD[Int], walkers: RDD[(Long,
    (Array[Long],
      Array[(Long, Double)], Long,
      Int))]) = {
    routingTable.zipPartitions(walkers.partitionBy(partitioner)) {
      (_, iter2) =>
        iter2
    }
  }

  def filterUnfinishedWalkers(walkers: RDD[(Long, (Array[Long], Array[(Long, Double)], Long, Int)
    )], walkLength: Broadcast[Int]) = {
    walkers.filter(_._2._4 < walkLength.value)
  }

  def prepareWalkersToTransfer(walkers: RDD[(Long, (Array[Long], Array[(Long, Double)], Long,
    Int))]) = {
    walkers.mapPartitions({
      iter =>
        iter.map {
          case (_, (steps,
          prevNeighbors, origin,
          stepCounter)) =>
            (steps.last, (steps.slice(steps.length - 2, steps.length), prevNeighbors, origin,
              stepCounter))
        }
    }, preservesPartitioning = false)

  }

  def mergeNewPaths(paths: RDD[(Long, (Array[Long], Int))], newPaths: RDD[(Long, (Array[Long],
    Array[(Long, Double)], Long, Int))], walkLength: Broadcast[Int]) = {
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
