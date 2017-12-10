package au.csiro.data61.randomwalk

import au.csiro.data61.randomwalk.common.{Params, Property}
import org.apache.log4j.LogManager
import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.rdd.RDD

import scala.util.Random

trait RandomWalk[T] {


  protected val context: SparkContext
  protected val config: Params
  lazy val partitioner: HashPartitioner = new HashPartitioner(config.rddPartitions)
  var routingTable: RDD[Int] = _
  lazy val logger = LogManager.getLogger("rwLogger")
  var nVertices: Int = 0
  var nEdges: Int = 0

  def execute(): RDD[List[Int]] = {
    randomWalk(loadGraph())
  }

  def save(paths: RDD[List[Int]], partitions: Int, output: String) = {

    paths.map {
      case (path) =>
        val pathString = path.mkString("\t")
        s"$pathString"
    }.repartition(partitions).saveAsTextFile(s"${output}.${Property.pathSuffix}")
  }

  def loadGraph(): RDD[T]

  def randomWalk(g: RDD[T], nextFloat: () => Float = Random.nextFloat): RDD[List[Int]]
}
