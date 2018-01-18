package au.csiro.data61.randomwalk.utils

import org.apache.log4j.LogManager
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object GraphFileManager {

  private lazy val logger = LogManager.getLogger("myLogger")

  def createUniqueIds(inputFiles: Array[String], output: String, sc: SparkContext): Unit = {
    var allFiles: RDD[(Int, String)] = sc.emptyRDD

    for (i <- 0 until inputFiles.length) {
      val bcTypeId = sc.broadcast(i)
      allFiles = allFiles.union(sc.textFile(inputFiles(i)).map(s => (bcTypeId.value, s)))
      val count = allFiles.count()
      logger.info(s"Total number of vertices: $count")
      println(s"Total number of vertices: $count")
    }

    allFiles.zipWithIndex().sortBy(_._2).map { case ((s, fType), uId) => s"$uId\t$fType\t$s" }
      .repartition(1).saveAsTextFile(output)

  }

  def query(input: String, id: Int, sc: SparkContext): Unit = {
    val bcId = sc.broadcast(id)
    val result = sc.textFile(input).map { line =>
      val columns = line.split("\\s+")
      (columns(0).toInt, line)
    }.filter(_._1 == bcId.value).collect()
    println(s"Number of hits: ${result.length}")
    if (!result.isEmpty) {
      println(s"Result: ${result.head._2}")
    }
  }

  /**
    * The first column must be vId and the last column must be vType.
    *
    * @param input
    * @param output
    * @param sc
    */
  def extractVertexTypeMap(input: String, output: String, sc: SparkContext): Unit = {
    sc.textFile(input).map { line =>
      val columns = line.split("\\s+")
      val vuId = columns.head
      val voId = columns(1)
      val vType = columns.last
      s"$vuId\t$vType\t$voId"
    }.repartition(1).saveAsTextFile(output)
  }

  def mapEdgeFileIds(edgeFile: String, typeMapFile: String, output: String,
                     srcDstTypes: Array[Int], sc: SparkContext)
  : Unit = {
    val bcSrcDstTypes = sc.broadcast(srcDstTypes)
    val edges = sc.textFile(edgeFile).map { line =>
      val columns = line.split("\\s+")
      val src = columns(0)
      val dst = columns(1)
      ((src.toInt, bcSrcDstTypes.value(0)), (dst.toInt, bcSrcDstTypes.value(1)))
    }
    println(s"Number of edges before mapping: ${edges.count}")

    val typeMap = sc.textFile(typeMapFile).map { line =>
      val columns = line.split("\\s+")
      val vuId = columns(0)
      val vType = columns(1)
      val voId = columns(2)
      ((voId.toInt, vType.toInt), vuId.toInt)
    }

    val indexedEdges = edges.join(typeMap).map { case ((src, srcType), ((dst, dstType), srcUId)) =>
      ((dst, dstType), (src, srcType, srcUId))
    }.join(typeMap).map { case ((dst, dstType), ((src, srcType, srcUId), dstUId)) =>
      s"$srcUId\t$dstUId\t$src\t$dst\t$srcType\t$dstType"
    }

    println(s"Number of edges after mapping: ${indexedEdges.count}")

    indexedEdges.repartition(1).saveAsTextFile(output)
  }

  def mergeEdgeFiles(inputs: Array[String], output: String, sc: SparkContext): Unit = {
    var allFiles: RDD[String] = sc.emptyRDD

    for (f <- inputs) {
      allFiles = allFiles.union(
        sc.textFile(f).map { line =>
          val columns = line.split("\\s+")
          val srcUId = columns(0)
          val dstUId = columns(1)
          s"$srcUId\t$dstUId"
        })
      val count = allFiles.count()
      println(s"Total number of edges: $count")
    }

    allFiles.repartition(1).saveAsTextFile(output)
  }
}
