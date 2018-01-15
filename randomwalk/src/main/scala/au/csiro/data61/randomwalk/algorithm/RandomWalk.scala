package au.csiro.data61.randomwalk.algorithm

import au.csiro.data61.randomwalk.common.{Params, Property}
import org.apache.log4j.LogManager
import org.apache.spark.broadcast.Broadcast
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
  lazy val metaPath: Array[Short] = config.metaPath.split("\\s+").map(t => t.toShort)


  def execute(): RDD[Array[Int]] = {
    var homo = false
    if (config.vTypeInput == null)
      homo = true
    val bcmp = context.broadcast(metaPath)
    randomWalk(loadGraph(homo, bcmp), bcMetapath = bcmp)
  }

  def loadGraph(homogeneous: Boolean, bcMetapath: Broadcast[Array[Short]]): RDD[(Int, Array[Int])]

  def loadNodeTypes(): RDD[(Int, Short)] = {
    config.vTypeInput match {
      case null => null
      case vFile =>
        context.textFile(vFile, minPartitions = config.rddPartitions).map { line =>
          val parts = line.split("\\s+")

          (parts.head.toInt, parts(1).toShort)
        }
    }
  }

  def initFirstStep(paths: RDD[(Int, Array[Int])], nextFloat: () =>
    Float = Random.nextFloat, bcMetapath: Broadcast[Array[Short]])
  : RDD[(Int, (Array[Int], Array[(Int, Float)], Boolean, Short))] = {
    paths.mapPartitions({ iter =>
      val mp = bcMetapath.value
      val mpIndex: Short = 1
      val nextMpIndex: Short = ((mpIndex + 1) % mp.length).toShort
      iter.map { case (pId, path: Array[Int]) =>
        // metapath index is 1 in the first step
        val neighbors = HGraphMap.getNeighbors(path.head, mp(mpIndex))
        if (neighbors != null && neighbors.length > 0) {
          val (nextStep, _) = RandomSample(nextFloat).sample(neighbors)
          (pId, (path ++ Array(nextStep), neighbors, false, nextMpIndex))
        } else {
          // It's a deadend.
          (pId, (path, Array.empty[(Int, Float)], true, nextMpIndex))
        }
      }
    }, preservesPartitioning = true
    )
  }

  def randomWalk(initPaths: RDD[(Int, Array[Int])], nextFloat: () => Float = Random
    .nextFloat, bcMetapath: Broadcast[Array[Short]])
  : RDD[Array[Int]] = {
    val bcP = context.broadcast(config.p)
    val bcQ = context.broadcast(config.q)
    val walkLength = context.broadcast(config.walkLength)
    var totalPaths: RDD[Array[Int]] = context.emptyRDD[Array[Int]]

    for (_ <- 0 until config.numWalks) {
      var unfinishedWalkers: RDD[(Int, (Array[Int], Array[(Int, Float)], Boolean, Short))] =
        initFirstStep(initPaths, nextFloat, bcMetapath)
      var pathsPieces: RDD[Array[Int]] = context.emptyRDD[Array[Int]].repartition(config
        .rddPartitions)
      var remainingWalkers = Int.MaxValue

      val acc = context.longAccumulator("Error finder")
      val acc2 = context.longAccumulator("Error finder")
      do {
        unfinishedWalkers = transferWalkersToTheirPartitions(routingTable,
          prepareWalkersToTransfer(unfinishedWalkers))

        unfinishedWalkers = unfinishedWalkers.mapPartitions({ iter =>
          val mp = bcMetapath.value
          iter.map { case (pId, (steps: Array[Int], prevNeighbors: Array[(Int, Float)],
          completed: Boolean, mpIndex: Short)) =>
            var path = steps
            var isCompleted = completed
            val rSample = RandomSample(nextFloat)
            var pNeighbors: Array[(Int, Float)] = null
            var currNeighbors: Array[(Int, Float)] = null
            var mpi = mpIndex
            breakable {
              while (!isCompleted && path.length != walkLength.value + 2) {
                if (path.length == steps.length) {
                  pNeighbors = prevNeighbors
                } else { // If the walker is continuing on the local
                  // partition.
                  pNeighbors = currNeighbors
                }
                currNeighbors = HGraphMap.getNeighbors(path.last, mp(mpi))
                mpi = ((mpi + 1) % mp.length).toShort
                if (currNeighbors != null) {
                  if (currNeighbors.length > 0) {
                    val (nextStep, _) = rSample.secondOrderSample(bcP.value.toFloat, bcQ.value
                      .toFloat, path(path.length - 2), pNeighbors, currNeighbors)
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

            (pId, (path, pNeighbors, isCompleted, mpi))
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
      totalPaths = totalPaths.union(pathsPieces).persist(StorageLevel
        .MEMORY_AND_DISK)

      totalPaths.count()

    }

    totalPaths
  }

  def transferWalkersToTheirPartitions(routingTable: RDD[Int], walkers: RDD[(Int, (Array[Int],
    Array[(Int, Float)], Boolean, Short))]) = {
    routingTable.zipPartitions(walkers.partitionBy(partitioner)) {
      (_, iter2) =>
        iter2
    }
  }

  def filterUnfinishedWalkers(walkers: RDD[(Int, (Array[Int], Array[(Int, Float)], Boolean,
    Short))]) = {
    walkers.filter(!_._2._3)
  }

  def filterCompletedPaths(walkers: RDD[(Int, (Array[Int], Array[(Int, Float)], Boolean, Short))
    ]) = {
    walkers.filter(_._2._3).map { case (_, (paths, _, _, _)) =>
      paths
    }
  }

  def prepareWalkersToTransfer(walkers: RDD[(Int, (Array[Int], Array[(Int, Float)], Boolean,
    Short))]): RDD[(Int, (Array[Int], Array[(Int, Float)], Boolean, Short))]


  def save(paths: RDD[Array[Int]], partitions: Int, output: String) = {

    paths.map {
      case (path) =>
        val pathString = path.mkString("\t")
        s"$pathString"
    }.repartition(partitions).saveAsTextFile(s"${output}.${Property.pathSuffix}")
  }
}
