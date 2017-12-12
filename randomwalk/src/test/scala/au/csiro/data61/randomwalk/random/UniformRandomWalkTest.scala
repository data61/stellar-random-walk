package au.csiro.data61.randomwalk.random

import au.csiro.data61.randomwalk.RandomSample
import au.csiro.data61.randomwalk.common.Params
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.scalatest.BeforeAndAfter

class UniformRandomWalkTest extends org.scalatest.FunSuite with BeforeAndAfter {

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
    RandomGraphMap.reset
  }

  test("load graph as undirected") {
    val config = Params(input = "./src/test/graph/karate.txt", directed = false)
    val rw = UniformRandomWalk(sc, config)
    val paths = rw.loadGraph() // loadGraph(int)
    assert(rw.nEdges == 156)
    assert(rw.nVertices == 34)
    assert(paths.count() == 34)
    val vAcc = sc.longAccumulator("v")
    val eAcc = sc.longAccumulator("e")
    paths.coalesce(1).mapPartitions { iter =>
      vAcc.add(RandomGraphMap.getNumVertices)
      eAcc.add(RandomGraphMap.getNumEdges)
      iter
    }.first()
    assert(eAcc.sum == 156)
    assert(vAcc.sum == 34)
  }

  test("load graph as directed") {
    val config = Params(input = "./src/test/graph/karate.txt", directed = true)
    val rw = UniformRandomWalk(sc, config)
    val paths = rw.loadGraph()
    assert(rw.nEdges == 78)
    assert(rw.nVertices == 34)
    assert(paths.count() == 34)
    val vAcc = sc.longAccumulator("v")
    val eAcc = sc.longAccumulator("e")
    paths.coalesce(1).mapPartitions { iter =>
      vAcc.add(RandomGraphMap.getNumVertices)
      eAcc.add(RandomGraphMap.getNumEdges)
      iter
    }.first()
    assert(eAcc.sum == 78)
    assert(vAcc.sum == 34)
  }

  test("the first step of Random Walk") {
    val config = Params(input = "./src/test/graph/testgraph.txt", directed = true)
    val rw = UniformRandomWalk(sc, config)
    val paths = rw.loadGraph()
    val result = rw.initFirstStep(paths)
    assert(result.count == paths.count())
    for (t <- result.collect()) {
      val p = t._2._1
      if (p.length == 2) {
        assert(p.head == 1)
        assert(p sameElements Array(1, 2))
      }
      else {
        assert(p.head == 2)
        assert(p sameElements Array(2))
      }
    }
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


  test("transferWalkersToTheirPartitions") {
    val v1 = (1, (Array.empty[(Int, Float)]))
    val v2 = (2, (Array.empty[(Int, Float)]))
    val v3 = (3, (Array.empty[(Int, Float)]))
    val numPartitions = 8

    val config = Params(rddPartitions = numPartitions)
    val rw = UniformRandomWalk(sc, config)
    val partitioner = rw.partitioner
    val graph = sc.parallelize(Array(v1, v2, v3)).partitionBy(partitioner)
    val rTable = rw.buildRoutingTable(graph)

    val w1 = (1, (Array.empty[Int], Array.empty[(Int, Float)], 1))
    val w2 = (2, (Array.empty[Int], Array.empty[(Int, Float)], 2))
    val w3 = (3, (Array.empty[Int], Array.empty[(Int, Float)], 3))

    val walkers = sc.parallelize(Array(w3, w1, w2)).partitionBy(partitioner)
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

    val p1 = Array(1, 2, 3, 4)
    val op1 = 1
    val wl1 = 5
    val w1 = (op1, (p1, Array.empty[(Int, Float)], wl1))
    val p2 = Array(2, 5)
    val op2 = 2
    val wl2 = 2
    val w2 = (op2, (p2, Array.empty[(Int, Float)], wl2))

    val walkers = sc.parallelize(Array(w1, w2))

    val rw = UniformRandomWalk(sc, Params())

    val preparedWalkers = rw.prepareWalkersToTransfer(walkers).collect()
    assert(preparedWalkers.length == 2)
    val l1 = preparedWalkers.filter(_._2._1.head == p1.head)(0)._2

    assert((l1._1 sameElements p1) && (l1._3 == wl1))
    //
    val l2 = preparedWalkers.filter(_._2._1.head == p2.head)(0)._2
    assert((l2._1 sameElements p2) && (l2._3 == wl2))
  }

  test("filterUnfinishedWalkers") {
    val walkLength = sc.broadcast(4)
    val p1 = (1, (Array.empty[Int], Array.empty[(Int, Float)], walkLength.value))
    val p2 = (2, (Array.empty[Int], Array.empty[(Int, Float)], walkLength.value - 2))
    val p3 = (3, (Array.empty[Int], Array.empty[(Int, Float)], walkLength.value - 1))
    val walkers = sc.parallelize(Array(p1, p2, p3))

    val rw = UniformRandomWalk(sc, Params())

    val filteredWalkers = rw.filterUnfinishedWalkers(walkers, walkLength).collect()

    assert(filteredWalkers.size == 2)
    assert(filteredWalkers.filter(_._1 == p2._1).length == 1)
    assert(filteredWalkers.filter(_._1 == p3._1).length == 1)

  }

  test("test 2nd order random walk undirected1") {
    // Undirected graph
    val rValue = 0.1f
    val wLength = 1
    val nextFloatGen = () => rValue
    val config = Params(input = "./src/test/graph/karate.txt", directed = false, walkLength =
      wLength, rddPartitions = 8, numWalks = 1)
    val rw = UniformRandomWalk(sc, config)
    val graph = rw.loadGraph()
    val paths = rw.randomWalk(graph, nextFloatGen)
    val rSampler = RandomSample(nextFloatGen)
    assert(paths.count() == rw.nVertices) // a path per vertex
    paths.collect().foreach { case (p: Array[Int]) =>
      val p2 = doSecondOrderRandomWalk(RandomGraphMap, p(0), wLength, rSampler, 1.0f, 1.0f)
      assert(p sameElements p2)
    }
  }

  test("test 2nd order random walk undirected2") {
    // Undirected graph
    val rValue = 0.1f
    val nextFloatGen = () => rValue
    val wLength = 50
    val config = Params(input = "./src/test/graph/karate.txt", directed = false, walkLength =
      wLength, rddPartitions = 8, numWalks = 1)
    val rw = UniformRandomWalk(sc, config)
    val graph = rw.loadGraph()
    val paths = rw.randomWalk(graph, nextFloatGen)
    assert(paths.count() == rw.nVertices) // a path per vertex
    val rSampler = RandomSample(nextFloatGen)
    paths.collect().foreach { case (p: Array[Int]) =>
      val p2 = doSecondOrderRandomWalk(RandomGraphMap, p(0), wLength, rSampler, 1.0f, 1.0f)
      assert(p sameElements p2)

    }
  }

  test("test 2nd order random walk undirected3") {
    // Undirected graph
    val wLength = 50
    val config = Params(input = "./src/test/graph/karate.txt", directed = false, walkLength =
      wLength, rddPartitions = 8, numWalks = 1)
    val rValue = 0.9f
    val nextFloatGen = () => rValue
    val rw = UniformRandomWalk(sc, config)
    val graph = rw.loadGraph()
    val paths = rw.randomWalk(graph, nextFloatGen)
    assert(paths.count() == rw.nVertices) // a path per vertex
    val rSampler = RandomSample(nextFloatGen)
    paths.collect().foreach { case (p: Array[Int]) =>
      val p2 = doSecondOrderRandomWalk(RandomGraphMap, p(0), wLength, rSampler, 1.0f, 1.0f)
      assert(p sameElements p2)
    }
  }

  test("test 2nd order random walk undirected4") {
    // Undirected graph
    val rValue = 0.1f
    val nextFloatGen = () => rValue
    val wLength = 50
    val config = Params(input = "./src/test/graph/karate.txt", directed = false, walkLength =
      wLength, rddPartitions = 8, numWalks = 1)
    val rw = UniformRandomWalk(sc, config)
    val graph = rw.loadGraph()
    val paths = rw.randomWalk(graph, nextFloatGen)
    assert(paths.count() == rw.nVertices) // a path per vertex
    val rSampler = RandomSample(nextFloatGen)
    paths.collect().foreach { case (p: Array[Int]) =>
      val p2 = doSecondOrderRandomWalk(RandomGraphMap, p(0), wLength, rSampler, 1.0f, 1.0f)
      assert(p sameElements p2)
    }
  }

  test("test 2nd order random walk directed1") {
    val wLength = 50
    val rValue = 0.9f
    val nextFloatGen = () => rValue

    // Directed Graph
    val config = Params(input = "./src/test/graph/karate.txt", directed = true, walkLength =
      wLength, rddPartitions = 8, numWalks = 1)
    val rw = UniformRandomWalk(sc, config)
    val graph = rw.loadGraph()
    val paths = rw.randomWalk(graph, nextFloatGen)
    assert(paths.count() == rw.nVertices) // a path per vertex
    val rSampler = RandomSample(nextFloatGen)
    paths.collect().foreach { case (p: Array[Int]) =>
      val p2 = doSecondOrderRandomWalk(RandomGraphMap, p(0), wLength, rSampler, 1.0f, 1.0f)
      assert(p sameElements p2)
    }
  }

  test("test 2nd order random walk directed2") {
    // Undirected graph
    val wLength = 50

    // Directed Graph
    val config = Params(input = "./src/test/graph/karate.txt", directed = true, walkLength =
      wLength, rddPartitions = 8, numWalks = 1)
    val rw = UniformRandomWalk(sc, config)
    val rValue = 0.1f
    val nextFloatGen = () => rValue
    val graph = rw.loadGraph()
    val paths = rw.randomWalk(graph, nextFloatGen)
    assert(paths.count() == rw.nVertices) // a path per vertex
    val rSampler = RandomSample(nextFloatGen)
    paths.collect().foreach { case (p: Array[Int]) =>
      val p2 = doSecondOrderRandomWalk(RandomGraphMap, p(0), wLength, rSampler, 1.0f, 1.0f)
      assert(p sameElements p2)
    }
  }

  private def doSecondOrderRandomWalk(gMap: RandomGraphMap.type, src: Int,
                                      walkLength: Int, rSampler: RandomSample, p: Float,
                                      q: Float): Array[Int]
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
