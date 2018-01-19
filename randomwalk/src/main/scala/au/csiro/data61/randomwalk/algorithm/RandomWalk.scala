package au.csiro.data61.randomwalk.algorithm

import au.csiro.data61.randomwalk.common.{Params, Property}
import org.apache.log4j.LogManager
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{HashPartitioner, SparkContext}

import scala.util.Random
import scala.util.control.Breaks.{break, breakable}

trait RandomWalk extends Serializable {

  protected val context: SparkContext
  protected val config: Params
  lazy val partitioner: HashPartitioner = new HashPartitioner(config.rddPartitions)
  var routingTable: RDD[Int] = _
  lazy val logger = LogManager.getLogger("rwLogger")
  var nVertices: Int = 0
  var nEdges: Int = 0

  def execute(): RDD[Array[Int]] = {
    randomWalk(loadGraph())
  }

  def loadGraph(): RDD[(Int, Array[Int])]


  def initFirstStep(paths: RDD[(Int, Array[Int])], nextFloat: () =>
    Float = Random.nextFloat): RDD[(Int, (Array[Int], Array[(Int, Float)], Boolean))] = {
    paths.mapPartitions({ iter =>
      val sample = RandomSample.sample(nextFloat)_
      iter.map { case (pId, path: Array[Int]) =>
        val neighbors = GraphMap.getNeighbors(path.head)
        if (neighbors != null && neighbors.length > 0) {
          val (nextStep, _) = sample(neighbors)
          (pId, (path ++ Array(nextStep), neighbors, false))
        } else {
          // It's a deaend.
          (pId, (path, Array.empty[(Int, Float)], true))
        }
      }
    }, preservesPartitioning = true
    )
  }

  def initFirstStepForAliasWalk(paths: RDD[(Int, Array[Int])], nextFloat: () =>
    Float = Random.nextFloat): RDD[(Int, (Array[Int], Boolean))] = {
    paths.mapPartitions({ iter =>
      val sample = RandomSample.sample(nextFloat)_
      iter.map { case (pId, path: Array[Int]) =>
        val neighbors = GraphMap.getNeighbors(path.head)
        if (neighbors != null && neighbors.length > 0) {
          val (nextStep, _) = sample(neighbors)
          (pId, (path ++ Array(nextStep), false))
        } else {
          // It's a deaend.
          (pId, (path, true))
        }
      }
    }, preservesPartitioning = true
    )
  }

  def memoryEfficientWalk(initPaths: RDD[(Int, Array[Int])],
                          nextFloat: () => Float = Random.nextFloat): RDD[Array[Int]] = {
    val bcP = context.broadcast(config.p)
    val bcQ = context.broadcast(config.q)
    val walkLength = context.broadcast(config.walkLength)
    var unfinishedWalkers: RDD[(Int, (Array[Int], Array[(Int, Float)], Boolean))] = initFirstStep(
      initPaths, nextFloat)
    var pathsPieces: RDD[Array[Int]] = context.emptyRDD[Array[Int]].repartition(config
      .rddPartitions)
    var remainingWalkers = Int.MaxValue

    val acc = context.longAccumulator("Error finder")
    val acc2 = context.longAccumulator("Error finder")
    do {
      unfinishedWalkers = transferWalkersToTheirPartitions(routingTable,
        prepareWalkersToTransfer(unfinishedWalkers))

      unfinishedWalkers = unfinishedWalkers.mapPartitions({ iter =>
        iter.map { case (pId, (steps: Array[Int], prevNeighbors: Array[(Int, Float)],
        completed: Boolean)) =>
          var path = steps
          var isCompleted = completed
          val rSample = RandomSample.secondOrderSample(nextFloat)_
          var pNeighbors: Array[(Int, Float)] = prevNeighbors
          breakable {
            while (!isCompleted && path.length != walkLength.value + 2) {
              val currNeighbors = GraphMap.getNeighbors(path.last)
              val prev = path(path.length - 2)
              if (path.length > steps.length) { // If the walker is continuing on the local
                // partition.
                pNeighbors = GraphMap.getNeighbors(prev)
              }
              if (currNeighbors != null) {
                if (currNeighbors.length > 0) {
                  val (nextStep, _) = rSample(bcP.value.toFloat, bcQ.value.toFloat, prev,
                    pNeighbors, currNeighbors)
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

    val pCount = pathsPieces.count()
    if (pCount != nVertices) {
      println(s"Inconsistent number of paths: nPaths=[${pCount}] != vertices[$nVertices]")
    }
    pathsPieces
  }

  def aliasSamplingWalk(initPaths: RDD[(Int, Array[Int])], nextFloat: () => Float): _root_.org
  .apache.spark.rdd.RDD[_root_.scala.Array[Int]] = {
    val bcP = context.broadcast(config.p)
    val bcQ = context.broadcast(config.q)
    val walkLength = context.broadcast(config.walkLength)
    var unfinishedWalkers: RDD[(Int, (Array[Int], Boolean))] = initFirstStepForAliasWalk(
      initPaths, nextFloat)
    var pathsPieces: RDD[Array[Int]] = context.emptyRDD[Array[Int]].repartition(config
      .rddPartitions)
    var remainingWalkers = Int.MaxValue

    val acc = context.longAccumulator("Error finder")
    val acc2 = context.longAccumulator("Error finder")
    do {
      unfinishedWalkers = transferWalkersToTheirPartitionsForAliasWalk(routingTable,
        prepareWalkersToTransferForAliasWalk(unfinishedWalkers))

      unfinishedWalkers = unfinishedWalkers.mapPartitions({ iter =>
        iter.map { case (pId, (steps: Array[Int], completed: Boolean)) =>
          var path = steps
          var isCompleted = completed
          val rand = RandomSample.rand(nextFloat)_
          breakable {
            while (!isCompleted && path.length != walkLength.value + 2) {
              val curr = path.last
              if (GraphMap.getNeighbors(curr) != null) {
                val prev = path(path.length - 2)
                val aliasTuple = GraphMap.getAliasTuple(prev, curr)
                if (aliasTuple != null) {
                  val index = rand(aliasTuple._1, aliasTuple._2)
                  val (nextStep, _) = GraphMap.getDestination(curr, index)
                  path = path ++ Array(nextStep)
                } else {
                  isCompleted = true
                  acc2.add(1)
                  break
                  // This walker has reached a deadend. Needs to stop.
                }
              }
              else {
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

          (pId, (path, isCompleted))
        }
      }
        , preservesPartitioning = true
      ).persist(StorageLevel.MEMORY_AND_DISK)

      pathsPieces = pathsPieces.union(filterCompletedPathsForAliasWalk(unfinishedWalkers))
        .persist(StorageLevel.MEMORY_AND_DISK)
      pathsPieces.count()

      unfinishedWalkers = filterUnfinishedWalkersForAliasWalk(unfinishedWalkers)

      val oldCount = remainingWalkers
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

    val pCount = pathsPieces.count()
    if (pCount != nVertices) {
      println(s"Inconsistent number of paths: nPaths=[${pCount}] != vertices[$nVertices]")
    }
    pathsPieces
  }

  def randomWalk(initPaths: RDD[(Int, Array[Int])], nextFloat: () => Float = Random
    .nextFloat): RDD[Array[Int]] = {
    var totalPaths: RDD[Array[Int]] = context.emptyRDD[Array[Int]]

    for (_ <- 0 until config.numWalks) {
      val paths: RDD[Array[Int]] = config.aliasSampling match {
        case false => memoryEfficientWalk(initPaths, nextFloat)
        case true => aliasSamplingWalk(initPaths, nextFloat)
      }
      totalPaths = totalPaths.union(paths).persist(StorageLevel
        .MEMORY_AND_DISK)

      totalPaths.count()

    }

    totalPaths
  }

  def transferWalkersToTheirPartitions(routingTable: RDD[Int], walkers: RDD[(Int, (Array[Int],
    Array[(Int, Float)], Boolean))]) = {
    routingTable.zipPartitions(walkers.partitionBy(partitioner)) {
      (_, iter2) =>
        iter2
    }
  }

  def transferWalkersToTheirPartitionsForAliasWalk(routingTable: RDD[Int], walkers: RDD[(Int,
    (Array[Int], Boolean))]) = {
    routingTable.zipPartitions(walkers.partitionBy(partitioner)) {
      (_, iter2) =>
        iter2
    }
  }

  def filterUnfinishedWalkers(walkers: RDD[(Int, (Array[Int], Array[(Int, Float)], Boolean))]) = {
    walkers.filter(!_._2._3)
  }

  def filterUnfinishedWalkersForAliasWalk(walkers: RDD[(Int, (Array[Int], Boolean))]) = {
    walkers.filter(!_._2._2)
  }

  def filterCompletedPaths(walkers: RDD[(Int, (Array[Int], Array[(Int, Float)], Boolean))]) = {
    walkers.filter(_._2._3).map { case (_, (paths, _, _)) =>
      paths
    }
  }

  def filterCompletedPathsForAliasWalk(walkers: RDD[(Int, (Array[Int], Boolean))]) = {
    walkers.filter(_._2._2).map { case (_, (paths, _)) =>
      paths
    }
  }

  def prepareWalkersToTransfer(walkers: RDD[(Int, (Array[Int], Array[(Int, Float)], Boolean))])
  : RDD[(Int, (Array[Int], Array[(Int, Float)], Boolean))]

  def prepareWalkersToTransferForAliasWalk(walkers: RDD[(Int, (Array[Int], Boolean))]): RDD[(Int,
    (Array[Int], Boolean))]

  def save(paths: RDD[Array[Int]], partitions: Int, output: String) = {

    paths.map {
      case (path) =>
        val pathString = path.mkString("\t")
        s"$pathString"
    }.repartition(partitions).saveAsTextFile(s"${output}.${Property.pathSuffix}")
  }
}
