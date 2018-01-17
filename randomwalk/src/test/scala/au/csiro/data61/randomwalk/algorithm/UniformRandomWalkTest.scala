package au.csiro.data61.randomwalk.algorithm

import au.csiro.data61.randomwalk.Main
import au.csiro.data61.randomwalk.common.CommandParser.TaskName
import au.csiro.data61.randomwalk.common.Params
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.scalatest.BeforeAndAfter


class UniformRandomWalkTest extends org.scalatest.FunSuite with BeforeAndAfter {

  private val karate = "./randomwalk/src/test/resources/karate.txt"
  private val testGraph = "./randomwalk/src/test/resources/testgraph.txt"
  private val master = "local[*]" // Note that you need to verify unit tests in a multi-core
  // computer.
  private val appName = "rw-unit-test"
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
    GraphMap.reset
  }

  test("load graph as undirected") {
    val config = Params(input = karate, directed = false)
    val rw = UniformRandomWalk(sc, config)
    val paths = rw.loadGraph() // loadGraph(int)
    assert(rw.nEdges == 156)
    assert(rw.nVertices == 34)
    assert(paths.count() == 34)
    val vAcc = sc.longAccumulator("v")
    val eAcc = sc.longAccumulator("e")
    paths.coalesce(1).mapPartitions { iter =>
      vAcc.add(GraphMap.getNumVertices)
      eAcc.add(GraphMap.getNumEdges)
      iter
    }.first()
    assert(eAcc.sum == 156)
    assert(vAcc.sum == 34)
  }

  test("load graph as directed") {
    val config = Params(input = karate, directed = true)
    val rw = UniformRandomWalk(sc, config)
    val paths = rw.loadGraph()
    assert(rw.nEdges == 78)
    assert(rw.nVertices == 34)
    assert(paths.count() == 34)
    val vAcc = sc.longAccumulator("v")
    val eAcc = sc.longAccumulator("e")
    paths.coalesce(1).mapPartitions { iter =>
      vAcc.add(GraphMap.getNumVertices)
      eAcc.add(GraphMap.getNumEdges)
      iter
    }.first()
    assert(eAcc.sum == 78)
    assert(vAcc.sum == 34)
  }

  test("buildRoutingTable") {
    val v1 = (1, (Array.empty[(Int, Float)]))
    val v2 = (2, (Array.empty[(Int, Float)]))
    val v3 = (3, (Array.empty[(Int, Float)]))
    val numPartitions = 3
    val partitioner = new HashPartitioner(numPartitions)

    val graph = sc.parallelize(Array(v1, v2, v3)).partitionBy(partitioner)

    val config = Params(rddPartitions = numPartitions)
    val rw = UniformRandomWalk(sc, config)
    val rTable = rw.buildRoutingTable(graph)
    assert(rTable.getNumPartitions == numPartitions)
    assert(rTable.partitioner match {
      case Some(p) => p equals partitioner
      case None => false
    })
    val ps = rTable.partitions
    assert(ps.length == numPartitions)
    assert(rTable.collect().isEmpty)
  }

  test("first order walk") {
    // Undirected graph
    val wLength = 50

    // Directed Graph
    val config = Params(input = karate, directed = false, walkLength =
      wLength, rddPartitions = 8, numWalks = 1)
    val rw = UniformRandomWalk(sc, config)
    val rValue = 0.1f
    val nextFloatGen = () => rValue
    val graph = rw.loadGraph()
    val paths = rw.firstOrderWalk(graph, nextFloatGen)
    assert(paths.count() == rw.nVertices) // a path per vertex
    val rSampler = RandomSample(nextFloatGen)
    paths.collect().foreach { case (p: Array[Int]) =>
      val p2 = doFirstOrderRandomWalk(GraphMap, p(0), wLength, rSampler)
      assert(p sameElements p2)
    }
  }

  test("Query Nodes") {
    val config = Params(nodes = "1 2 3 4")

    val p1 = Array(1, 2, 1, 2, 1)
    val p2 = Array(2, 2, 2, 2, 1)
    val p3 = Array(3, 4, 2, 3)
    val p4 = Array(4)

    val paths = sc.parallelize(Array(p1, p2, p3, p4))
    val rw = UniformRandomWalk(sc, config)
    val counts = rw.queryPaths(paths)
    assert(counts sameElements Array((1, (4, 2)), (2, (7, 3)), (3, (2, 1)), (4, (2, 2))))
  }

  test("Experiments") {
    val query = 1 to 34 toArray
    val config = Params(input = "",
      output = "", directed = false, walkLength = 10,
      rddPartitions = 8, numWalks = 1, cmd = TaskName.firstorder, nodes = query.mkString(" "))

    Main.execute(sc, config)
  }

  private def doFirstOrderRandomWalk(gMap: GraphMap.type, src: Int,
                                     walkLength: Int, rSampler: RandomSample): Array[Int]
  = {
    var path = Array(src)

    for (_ <- 0 until walkLength) {

      val curr = path.last
      val currNeighbors = gMap.getNeighbors(curr)
      if (currNeighbors.length > 0) {
        path = path ++ Array(rSampler.sample(currNeighbors)._1)
      } else {
        return path
      }
    }

    path
  }

}
