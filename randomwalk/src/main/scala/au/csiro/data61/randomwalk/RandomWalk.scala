package au.csiro.data61.randomwalk

import au.csiro.data61.randomwalk.common.{Params, Property}
import org.apache.log4j.LogManager
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkContext}

import scala.util.Random

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

  def initFirstStep(g: RDD[(Int, Array[Int])] , nextFloat: () => Float = Random.nextFloat): RDD[(Int,
    (Array[Int], Array[(Int, Float)], Int))]

  def randomWalk(initPaths: RDD[(Int, Array[Int])], nextFloat: () => Float = Random.nextFloat): RDD[Array[Int]]

  def transferWalkersToTheirPartitions(routingTable: RDD[Int], walkers: RDD[(Int, (Array[Int],
    Array[(Int, Float)], Int))]) = {
    routingTable.zipPartitions(walkers.partitionBy(partitioner)) {
      (_, iter2) =>
        iter2
    }
  }

  def filterUnfinishedWalkers(walkers: RDD[(Int, (Array[Int], Array[(Int, Float)], Int))],
                              walkLength: Broadcast[Int]) = {
    walkers.filter(_._2._3 < walkLength.value)
  }

  def filterCompletedPaths(walkers: RDD[(Int, (Array[Int], Array[(Int, Float)], Int))],
                           walkLength: Broadcast[Int]) = {
    walkers.filter(_._2._3 == walkLength.value).map { case (_, (paths, _, _)) =>
      paths
    }
  }

  def prepareWalkersToTransfer(walkers: RDD[(Int, (Array[Int], Array[(Int, Float)], Int))]): RDD[
    (Int, (Array[Int], Array[(Int, Float)], Int))]

  def save(paths: RDD[Array[Int]], partitions: Int, output: String) = {

    paths.map {
      case (path) =>
        val pathString = path.mkString("\t")
        s"$pathString"
    }.repartition(partitions).saveAsTextFile(s"${output}.${Property.pathSuffix}")
  }
}
