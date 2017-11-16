package au.csiro.data61.randomwalk.efficient

import au.csiro.data61.Main.Params
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.scalatest.BeforeAndAfter

import scala.collection.Map

class RandomWalkTest extends org.scalatest.FunSuite with BeforeAndAfter {

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
    val config = Params(input = "./src/test/graph/karate.txt", directed = false)
    val rw = RandomWalk(sc, config)
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
    val config = Params(input = "./src/test/graph/karate.txt", directed = true)
    val rw = RandomWalk(sc, config)
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

  test("the first step of Random Walk") {
    val config = Params(input = "./src/test/graph/testgraph.txt", directed = true)
    val rw = RandomWalk(sc, config)
    val paths = rw.loadGraph()
    val result = rw.doFirsStepOfRandomWalk(paths)
    assert(result.count == paths.count())
    for (t <- result.collect()) {
      val p = t._2._1
      if (p.length == 2) {
        assert(p.head == 1)
        assert(p sameElements Array(1L, 2L))
      }
      else {
        assert(p.head == 2)
        assert(p sameElements Array(2L))
      }
    }
  }

  test("buildRoutingTable") {
    val v1 = (1L, Array.empty[Long])
    val v2 = (2L, Array.empty[Long])
    val v3 = (3L, Array.empty[Long])
    val numPartitions = 3
    val partitioner = new HashPartitioner(numPartitions)

    val graph = sc.parallelize(Array(v1, v2, v3)).partitionBy(partitioner)

    val config = Params(rddPartitions = numPartitions)
    val rw = RandomWalk(sc, config)
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


  test("transferWalkersToTheirPartitions") {

    val v1 = (1L, Array.empty[Long])
    val v2 = (2L, Array.empty[Long])
    val v3 = (3L, Array.empty[Long])
    val numPartitions = 8

    val config = Params(rddPartitions = numPartitions)
    val rw = RandomWalk(sc, config)
    val partitioner = rw.partitioner
    val graph = sc.parallelize(Array(v1, v2, v3)).partitionBy(partitioner)
    val rTable = rw.buildRoutingTable(graph)

    val w1 = (1L, (Array.empty[Long], Array.empty[(Long, Double)], 1L, 1))
    val w2 = (2L, (Array.empty[Long], Array.empty[(Long, Double)], 2L, 2))
    val w3 = (3L, (Array.empty[Long], Array.empty[(Long, Double)], 3L, 3))

    val walkers = sc.parallelize(Array(w3, w1, w2))
    val tWalkers = rw.transferWalkersToTheirPartitions(rTable, walkers)
    assert(tWalkers.getNumPartitions == numPartitions)

    for (i <- 0 until numPartitions) {
      val pw = tWalkers.mapPartitionsWithIndex((id, iter) => if (id == i) iter else
        Iterator())
        .collect()
      val pg = graph.mapPartitionsWithIndex((id, iter) => if (id == i) iter else Iterator())
        .collect()
      pw.foreach(w => assert(pg.exists(p => p._1 == w._1)))
    }
  }

  test("prepareWalkersToTransfer") {

    val p11 = Array(1L, 2L)
    val last1 = 4L
    val p12 = Array(3L, last1)
    val op1 = 1L
    val wl1 = 5
    val w1 = (op1, (p11 ++ p12, Array.empty[(Long, Double)], op1, wl1))
    val p21 = Array.empty[Long]
    val last2 = 5L
    val p22 = Array(2L, last2)
    val op2 = 2L
    val wl2 = 2
    val w2 = (op2, (p21 ++ p22, Array.empty[(Long, Double)], op2, wl2))

    val walkers = sc.parallelize(Array(w1, w2))

    val rw = RandomWalk(sc, Params())

    val preparedWalkers = rw.prepareWalkersToTransfer(walkers)
    assert(preparedWalkers.count() == 2)
    val map = preparedWalkers.collectAsMap()
    assert(map.get(last1) match {
      case Some(w) => (w._1 sameElements p12) && (w._3 == op1) && (w._4 == wl1)
      case None => false
    })

    assert(map.get(last2) match {
      case Some(w) => (w._1 sameElements p22) && (w._3 == op2) && (w._4 == wl2)
      case None => false
    })
  }

  test("test mergeNewPaths") {

    //merge with empty paths

    var paths = sc.emptyRDD[(Long, (Array[Long], Int))]
    val walkLength = sc.broadcast(4)
    val fP1 = Array(1L, 2L, 3L)
    val oP1 = 1L
    val p1 = (1L, (fP1, Array.empty[(Long, Double)], oP1, walkLength.value))
    val p21 = Array(2L)
    val p22 = Array(4L, 5L)
    val oP2 = 2L
    val wl21 = 3
    var p2 = (2L, (p21 ++ p22, Array.empty[(Long, Double)], oP2, wl21))
    var newPaths = sc.parallelize(Array(p1, p2))

    val rw = RandomWalk(sc, Params())

    paths = rw.mergeNewPaths(paths, newPaths, walkLength)
    assert(paths.count() == 2)
    val merged: Map[Long, (Array[Long], Int)] = paths.collectAsMap()
    assert(merged.get(oP1) match {
      case Some(p) => (p._1 sameElements fP1) && (p._2 == walkLength.value)
      case None => false
    })

    assert(merged.get(oP2) match {
      case Some(p) => (p._1 sameElements (p21)) && (p._2 == wl21)
      case None => false
    })

    // Merge with a non-empty paths RDD
    val wl22 = 4
    val p23 = Array(6L)
    p2 = (2L, (p22 ++ p23, Array.empty[(Long, Double)], oP2, wl22))
    newPaths = sc.parallelize(Array(p2))

    paths = rw.mergeNewPaths(paths, newPaths, walkLength)
    assert(paths.count() == 3)
    val mergedArray = paths.collect()

    mergedArray.foreach { case (origin: Long, (steps: Array[Long], wl: Int)) =>
      if (origin == oP1)
        assert((steps sameElements (fP1)) && (wl == walkLength.value))
      else {
        assert(origin == oP2)
        if (steps.length == 1)
          assert((steps sameElements (p21)) && (wl == wl21))
        else
          assert((steps sameElements (p22 ++ p23)) && (wl == wl22))
      }
    }
  }

  test("sortPathPieces") {

    val w11 = (1L, (Array.empty[Long], 1))
    val w12 = (w11._1, (Array(1L, 2L), 2))
    val w13 = (w11._1, (Array(3L, 4L), 4))
    val w21 = (2L, (Array(2L, 5L), 1))
    val w22 = (w21._1, (Array(6L, 7L), 3))
    val w23 = (w21._1, (Array(8L, 2L), 5))
    val pieces = sc.parallelize(Array(w13, w11, w12, w22, w23, w21))

    val rw = RandomWalk(sc, Params())

    val sorted = rw.sortPathPieces(pieces)
    assert(sorted.count() == 2)

    val result = sorted.collect()
    result.foreach { p =>
      if (p(0) == w11._1) {
        assert(p sameElements (w11._2._1 ++ w12._2._1 ++ w13._2._1))
      } else {
        assert(p sameElements (w21._2._1 ++ w22._2._1 ++ w23._2._1))
      }
    }
  }

  test("filterUnfinishedWalkers") {
    val walkLength = sc.broadcast(4)
    val p1 = (1L, (Array.empty[Long], Array.empty[(Long, Double)], 1L, walkLength.value))
    val p2 = (2L, (Array.empty[Long], Array.empty[(Long, Double)], 2L, walkLength.value - 2))
    val p3 = (3L, (Array.empty[Long], Array.empty[(Long, Double)], 3L, walkLength.value - 1))
    val walkers = sc.parallelize(Array(p1, p2, p3))

    val rw = RandomWalk(sc, Params())

    val filteredWalkers = rw.filterUnfinishedWalkers(walkers, walkLength).collectAsMap()

    assert(filteredWalkers.size == 2)
    assert(filteredWalkers.contains(p2._1))
    assert(filteredWalkers.contains(p3._1))

  }

  test("test 2nd order random walk undirected1") {
    // Undirected graph
    val rValue = 0.1
    val wLength = 1
    val nextDoubleGen = () => rValue
    val config = Params(input = "./src/test/graph/karate.txt", directed = false, walkLength =
      wLength, rddPartitions = 8, numWalks = 1)
    val rw = RandomWalk(sc, config)
    val graph = rw.loadGraph()
    val paths = rw.randomWalk(graph, nextDoubleGen)
    val rSampler = RandomSample(nextDoubleGen)
    assert(paths.count() == rw.nVertices) // a path per vertex
    paths.collect().foreach { case (p: List[Long]) =>
      val p2 = doSecondOrderRandomWalk(GraphMap, p(0), wLength, rSampler, 1.0, 1.0)
      assert(p sameElements p2)
    }
  }

  test("test 2nd order random walk undirected2") {
    // Undirected graph
    val rValue = 0.1
    val nextDoubleGen = () => rValue
    val wLength = 50
    val config = Params(input = "./src/test/graph/karate.txt", directed = false, walkLength =
      wLength, rddPartitions = 8, numWalks = 1)
    val rw = RandomWalk(sc, config)
    val graph = rw.loadGraph()
    val paths = rw.randomWalk(graph, nextDoubleGen)
    assert(paths.count() == rw.nVertices) // a path per vertex
    val rSampler = RandomSample(nextDoubleGen)
    paths.collect().foreach { case (p: List[Long]) =>
      val p2 = doSecondOrderRandomWalk(GraphMap, p(0), wLength, rSampler, 1.0, 1.0)
      assert(p sameElements p2)

    }
  }

  test("test 2nd order random walk undirected3") {
    // Undirected graph
    val wLength = 50
    val config = Params(input = "./src/test/graph/karate.txt", directed = false, walkLength =
      wLength, rddPartitions = 8, numWalks = 1)
    val rValue = 0.9
    val nextDoubleGen = () => rValue
    val rw = RandomWalk(sc, config)
    val graph = rw.loadGraph()
    val paths = rw.randomWalk(graph, nextDoubleGen)
    assert(paths.count() == rw.nVertices) // a path per vertex
    val rSampler = RandomSample(nextDoubleGen)
    paths.collect().foreach { case (p: List[Long]) =>
      val p2 = doSecondOrderRandomWalk(GraphMap, p(0), wLength, rSampler, 1.0, 1.0)
      assert(p sameElements p2)
    }
  }

  test("test 2nd order random walk undirected4") {
    // Undirected graph
    val rValue = 0.1
    val nextDoubleGen = () => rValue
    val wLength = 50
    val config = Params(input = "./src/test/graph/karate.txt", directed = false, walkLength =
      wLength, rddPartitions = 8, numWalks = 1)
    val rw = RandomWalk(sc, config)
    val graph = rw.loadGraph()
    val paths = rw.randomWalk(graph, nextDoubleGen)
    assert(paths.count() == rw.nVertices) // a path per vertex
    val rSampler = RandomSample(nextDoubleGen)
    paths.collect().foreach { case (p: List[Long]) =>
      val p2 = doSecondOrderRandomWalk(GraphMap, p(0), wLength, rSampler, 1.0, 1.0)
      assert(p sameElements p2)
    }
  }

  test("test 2nd order random walk directed1") {
    val wLength = 50
    val rValue = 0.9
    val nextDoubleGen = () => rValue

    // Directed Graph
    val config = Params(input = "./src/test/graph/karate.txt", directed = true, walkLength =
      wLength, rddPartitions = 8, numWalks = 1)
    val rw = RandomWalk(sc, config)
    val graph = rw.loadGraph()
    val paths = rw.randomWalk(graph, nextDoubleGen)
    assert(paths.count() == rw.nVertices) // a path per vertex
    val rSampler = RandomSample(nextDoubleGen)
    paths.collect().foreach { case (p: List[Long]) =>
      val p2 = doSecondOrderRandomWalk(GraphMap, p(0), wLength, rSampler, 1.0, 1.0)
      assert(p sameElements p2)
    }
  }

  test("test 2nd order random walk directed2") {
    // Undirected graph
    val wLength = 50

    // Directed Graph
    val config = Params(input = "./src/test/graph/karate.txt", directed = true, walkLength =
      wLength, rddPartitions = 8, numWalks = 1)
    val rw = RandomWalk(sc, config)
    val rValue = 0.1
    val nextDoubleGen = () => rValue
    val graph = rw.loadGraph()
    val paths = rw.randomWalk(graph, nextDoubleGen)
    assert(paths.count() == rw.nVertices) // a path per vertex
    val rSampler = RandomSample(nextDoubleGen)
    paths.collect().foreach { case (p: List[Long]) =>
      val p2 = doSecondOrderRandomWalk(GraphMap, p(0), wLength, rSampler, 1.0, 1.0)
      assert(p sameElements p2)
    }
  }

  private def doSecondOrderRandomWalk(gMap: GraphMap.type, src: Long,
                                      walkLength: Int, rSampler: RandomSample, p: Double,
                                      q: Double): Array[Long]
  = {
    var path = Array(src)
    val neighbors = gMap.getNeighbors(src)
    if (neighbors.length > 0) {
      path = path ++ Array(rSampler.sample(neighbors)._1)
    }
    else {
      return path
    }

    for (_ <- 0 until walkLength) {

      val curr = path.last
      val prev = path(path.length - 2)
      val currNeighbors = gMap.getNeighbors(curr)
      if (currNeighbors.length > 0) {
        val prevNeighbors = gMap.getNeighbors(prev)
        path = path ++ Array(rSampler.secondOrderSample(p, q, prev, prevNeighbors, currNeighbors)
          ._1)
      } else {
        return path
      }
    }

    path
  }

}
