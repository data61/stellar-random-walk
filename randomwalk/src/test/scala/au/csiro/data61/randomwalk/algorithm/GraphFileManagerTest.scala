package au.csiro.data61.randomwalk.algorithm

import au.csiro.data61.randomwalk.utils.GraphFileManager
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.BeforeAndAfter

class GraphFileManagerTest extends org.scalatest.FunSuite with BeforeAndAfter {

  val AMINER_DIR = "~/net_aminer/"
  private val master = "local[*]" // Note that you need to verify unit tests in a multi-core
  // computer.
  private val appName = "gfm-unit-test"
  private var sc: SparkContext = _

  before {
    // Note that the Unit Test may throw "java.lang.AssertionError: assertion failed: Expected
    // hostname"
    // If this test is running in MacOS and without Internet connection.
    // https://issues.apache.org/jira/browse/SPARK-19394
    val conf = new SparkConf().setMaster(master).setAppName(appName)
    sc = SparkContext.getOrCreate(conf)
  }

  after {
    if (sc != null) {
      sc.stop()
    }
  }

  ignore("create unique Ids") {

    val f1 = AMINER_DIR + "id_author.txt" // t0
    val f2 = AMINER_DIR + "id_conf.txt" // t1
    val f3 = AMINER_DIR + "paper.txt" // t2
    val inputs = Array(f1, f2, f3)
    val output = AMINER_DIR + "/converted/acp.txt"
    GraphFileManager.createUniqueIds(inputs, output, sc)
  }

  ignore("create unique vertex type mapping file") {
    val f1 = AMINER_DIR + "/converted/acp.txt"
    val output = AMINER_DIR + "/converted/type-maps"
    GraphFileManager.extractVertexTypeMap(f1, output, sc = sc)
  }

  ignore("map edge file Ids to Unique Ids") {
    val edgeFile = AMINER_DIR + "paper_conf.txt"
    val typeFile = AMINER_DIR + "/converted/type-maps"
    val output = AMINER_DIR + "/converted/p-c-edges"
    GraphFileManager.mapEdgeFileIds(edgeFile, typeFile, output, Array(2, 1), sc)
  }

  ignore("merge edge files") {
    val f1 = AMINER_DIR + "/converted/p-a-edges"
    val f2 = AMINER_DIR + "/converted/p-c-edges"
    val inputs = Array(f1, f2)
    val output = AMINER_DIR + "/converted/aminer-all"
    GraphFileManager.mergeEdgeFiles(inputs, output, sc)
  }

  ignore("query") {
    val input = AMINER_DIR + "id_author.txt"
    GraphFileManager.query(input, id = 0, sc)
  }
}
