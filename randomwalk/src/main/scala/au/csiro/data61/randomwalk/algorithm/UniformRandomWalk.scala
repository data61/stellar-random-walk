package au.csiro.data61.randomwalk.algorithm

import au.csiro.data61.randomwalk.common.{Params, Property}
import org.apache.log4j.LogManager
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{HashPartitioner, SparkContext}

import scala.util.control.Breaks.{break, breakable}
import scala.util.{Random, Try}

case class UniformRandomWalk(context: SparkContext, config: Params) extends Serializable {

  lazy val partitioner: HashPartitioner = new HashPartitioner(config.rddPartitions)
  var routingTable: RDD[Int] = _
  lazy val logger = LogManager.getLogger("rwLogger")
  var nVertices: Int = 0
  var nEdges: Int = 0

  def execute(): RDD[Array[Int]] = {
    firstOrderWalk(loadGraph())
  }

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
      val (r, e) = GraphMap.getGraphStatsOnlyOnce
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

  def firstOrderWalk(initPaths: RDD[(Int, Array[Int])], nextFloat: () => Float = Random
    .nextFloat): RDD[Array[Int]] = {
    val walkLength = context.broadcast(config.walkLength)
    var totalPaths: RDD[Array[Int]] = context.emptyRDD[Array[Int]]

    for (_ <- 0 until config.numWalks) {
      val paths = initPaths.mapPartitions({ iter =>
        iter.map { case (_, steps) =>
          var path = steps
          val rSample = RandomSample(nextFloat)
          breakable {
            while (path.length < walkLength.value + 1) {
              val neighbors = GraphMap.getNeighbors(path.last)
              if (neighbors != null && neighbors.length > 0) {
                val (nextStep, _) = rSample.sample(neighbors)
                path = path ++ Array(nextStep)
              } else {
                break
              }
            }
          }
          path
        }
      }, preservesPartitioning = true
      ).persist(StorageLevel.MEMORY_AND_DISK)

      paths.count()

      val pCount = paths.count()
      if (pCount != nVertices) {
        println(s"Inconsistent number of paths: nPaths=[${pCount}] != vertices[$nVertices]")
      }
      totalPaths = totalPaths.union(paths).persist(StorageLevel
        .MEMORY_AND_DISK)

      totalPaths.count()

    }

    totalPaths
  }

  def buildRoutingTable(graph: RDD[(Int, Array[(Int, Float)])]): RDD[Int] = {

    graph.mapPartitionsWithIndex({ (id: Int, iter: Iterator[(Int, Array[(Int, Float)])]) =>
      iter.foreach { case (vId, neighbors) =>
        GraphMap.addVertex(vId, neighbors)
        id
      }
      Iterator.empty
    }, preservesPartitioning = true
    )

  }

  def save(paths: RDD[Array[Int]]): RDD[Array[Int]] = {

    paths.map {
      case (path) =>
        val pathString = path.mkString("\t")
        s"$pathString"
    }.repartition(config.rddPartitions).saveAsTextFile(s"${config.output}.${Property.pathSuffix}")
    paths
  }

  def queryPaths(paths: RDD[Array[Int]]): Array[(Int, (Int, Int))] = {
    val nodes = config.nodes.split("\\s+").map(s => s.toInt)
    val numOccurrences = new Array[(Int, (Int, Int))](nodes.length)

    for (i <- 0 until nodes.length) {
      val bcNode = context.broadcast(nodes(i))
      numOccurrences(i) = (nodes(i),
        paths.mapPartitions { iter =>
          val targetNode = bcNode.value
          iter.map { case steps =>
            val counts = steps.count(s => s == targetNode)
            val occurs = if (counts > 0) 1 else 0
            (counts, occurs)
          }
        }.reduce((c, o) => (c._1 + o._1, c._2 + o._2)))
    }

    numOccurrences
  }

}
