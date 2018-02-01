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
  /**
    * Routing table is for guiding Spark-engine to co-locate each random-walker with the
    * correct partition in the same executor.
    */
  var routingTable: RDD[Int] = _
  lazy val logger = LogManager.getLogger("rwLogger")
  var nVertices: Int = 0
  var nEdges: Int = 0

  /**
    * Loads a graph from filesystem and runs the randomwalk.
    *
    * @return An RDD of one/multiple array(s) of vertex-ids (sequence of vertices) per graph vertex.
    */
  def execute(): RDD[Array[Int]] = {
    randomWalk(loadGraph())
  }

  /**
    * Loads a graph from an edge list.
    *
    * @return an RDD of (srcId, Array(srcId)). Array(srcId) in fact contains the first step of
    *         the randomwalk that is the source vertex itself.
    */
  def loadGraph(): RDD[(Int, Array[Int])]

  /**
    * Initializes the first step of the randomwalk, that is a first-order randomwalk.
    *
    * @param paths
    * @param nextFloat random number generator. This enables to assign any type of random number
    *                  generator that can be used for test purposes as well.
    * @return a tuple including (partition-id, (path, current-vertex-neighbors, completed))
    */
  def initFirstStep(paths: RDD[(Int, Array[Int])], nextFloat: () =>
    Float = Random.nextFloat): RDD[(Int, (Array[Int], Array[(Int, Float)], Boolean))] = {
    paths.mapPartitions({ iter =>
      iter.map { case (pId, path: Array[Int]) =>
        val neighbors = GraphMap.getNeighbors(path.head)
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

  /**
    * The second-order randomwalk.
    *
    * @param initPaths
    * @param nextFloat
    * @return paths.
    */
  def randomWalk(initPaths: RDD[(Int, Array[Int])], nextFloat: () => Float = Random
    .nextFloat): RDD[Array[Int]] = {
    val bcP = context.broadcast(config.p)
    val bcQ = context.broadcast(config.q)
    val walkLength = context.broadcast(config.walkLength)
    var totalPaths: RDD[Array[Int]] = context.emptyRDD[Array[Int]]

    for (_ <- 0 until config.numWalks) {
      var unfinishedWalkers: RDD[(Int, (Array[Int], Array[(Int, Float)], Boolean))] = initFirstStep(
        initPaths, nextFloat)
      var completedPaths: RDD[Array[Int]] = context.emptyRDD[Array[Int]].repartition(config
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
            val rSample = RandomSample(nextFloat)
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
                    val (nextStep, _) = rSample.secondOrderSample(bcP.value.toFloat, bcQ.value
                      .toFloat, prev, pNeighbors, currNeighbors)
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

        completedPaths = completedPaths.union(filterCompletedPaths(unfinishedWalkers))
          .persist(StorageLevel.MEMORY_AND_DISK)
        completedPaths.count()

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

      val pCount = completedPaths.count()
      if (pCount != nVertices) {
        println(s"Inconsistent number of paths: nPaths=[${pCount}] != vertices[$nVertices]")
      }
      totalPaths = totalPaths.union(completedPaths).persist(StorageLevel
        .MEMORY_AND_DISK)

      totalPaths.count()

    }

    totalPaths
  }

  /**
    * Uses routing table to partition walkers RDD based on their key (partition-id) and locates
    * specific partition ids to specific executors.
    *
    * @param routingTable
    * @param walkers
    * @return
    */
  def transferWalkersToTheirPartitions(routingTable: RDD[Int], walkers: RDD[(Int, (Array[Int],
    Array[(Int, Float)], Boolean))]): RDD[(Int, (Array[Int], Array[(Int, Float)], Boolean))] = {
    routingTable.zipPartitions(walkers.partitionBy(partitioner)) {
      (_, iter2) =>
        iter2
    }
  }

  /**
    * Extracts the unfinished random-walkers.
    *
    * @param walkers
    * @return
    */
  def filterUnfinishedWalkers(walkers: RDD[(Int, (Array[Int], Array[(Int, Float)], Boolean))])
  : RDD[(Int, (Array[Int], Array[(Int, Float)], Boolean))] = {
    walkers.filter(!_._2._3)
  }

  /**
    * Extracts completed paths.
    *
    * @param walkers
    * @return completed paths.
    */
  def filterCompletedPaths(walkers: RDD[(Int, (Array[Int], Array[(Int, Float)], Boolean))])
  : RDD[Array[Int]] = {
    walkers.filter(_._2._3).map { case (_, (paths, _, _)) =>
      paths
    }
  }

  /**
    * Updates the partition-id (the key) for transferring the walker to its right destination.
    *
    * @param walkers
    * @return
    */
  def prepareWalkersToTransfer(walkers: RDD[(Int, (Array[Int], Array[(Int, Float)], Boolean))])
  : RDD[(Int, (Array[Int], Array[(Int, Float)], Boolean))]

  /**
    * Writes the given paths to the disk.
    *
    * @param paths
    * @param partitions
    * @param output
    */
  def save(paths: RDD[Array[Int]], partitions: Int, output: String): Unit = {

    paths.map {
      case (path) =>
        val pathString = path.mkString("\t")
        s"$pathString"
    }.repartition(partitions).saveAsTextFile(s"${output}/${Property.pathSuffix}")
  }
}
