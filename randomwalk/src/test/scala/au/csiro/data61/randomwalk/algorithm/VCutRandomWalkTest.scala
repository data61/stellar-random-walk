package au.csiro.data61.randomwalk.algorithm

import au.csiro.data61.randomwalk.common.Params
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.scalatest.BeforeAndAfter

class VCutRandomWalkTest extends org.scalatest.FunSuite with BeforeAndAfter with Serializable {

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
    HGraphMap.initGraphMap(1)
    val conf = new SparkConf().setMaster(master).setAppName(appName)
    sc = SparkContext.getOrCreate(conf)
  }

  after {
    if (sc != null) {
      sc.stop()
    }
  }

  test("load graph as undirected") {
    val config = Params(input = karate, directed = false)
    val rw = VCutRandomWalk(sc, config)
    val metaPath: Array[Short] = Array(0, 0)
    val paths = rw.loadGraph(hetero = false, sc.broadcast(metaPath))
    assert(rw.nEdges == 156)
    assert(rw.nVertices == 34)
    assert(paths.count() == 34)
    val vAcc = sc.longAccumulator("v")
    val eAcc = sc.longAccumulator("e")
    paths.coalesce(1).mapPartitions { iter =>
      vAcc.add(HGraphMap.getNumVertices(0))
      eAcc.add(HGraphMap.getNumEdges(0))
      iter
    }.first()
    assert(eAcc.sum == 156)
    assert(vAcc.sum == 34)
  }

  test("load graph as directed") {
    val config = Params(input = karate, directed = true)
    val rw = VCutRandomWalk(sc, config)
    val metaPath: Array[Short] = Array(0, 0)
    val paths = rw.loadGraph(hetero = false, sc.broadcast(metaPath))
    assert(rw.nEdges == 78)
    assert(rw.nVertices == 34)
    assert(paths.count() == 34)
    val vAcc = sc.longAccumulator("v")
    val eAcc = sc.longAccumulator("e")
    paths.coalesce(1).mapPartitions { iter =>
      vAcc.add(HGraphMap.getNumVertices(0))
      eAcc.add(HGraphMap.getNumEdges(0))
      iter
    }.first()
    assert(eAcc.sum == 78)
    assert(vAcc.sum == 34)
  }

  test("the first step of Random Walk") {
    val config = Params(input = testGraph, directed = true)
    val rw = VCutRandomWalk(sc, config)
    val metaPath: Array[Short] = Array(0, 0)
    val paths = rw.loadGraph(hetero = false, sc.broadcast(metaPath))

    val result = rw.initFirstStep(paths, bcMetapath = sc.broadcast(rw.metaPath))
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
    val vType: Short = 0
    val pId = 0
    val v1 = (pId, (1, Array.empty[(Int, Int, Float)], vType))
    val v2 = (pId, (2, Array.empty[(Int, Int, Float)], vType))
    val v3 = (pId, (3, Array.empty[(Int, Int, Float)], vType))
    val numPartitions = 3
    val partitioner = new HashPartitioner(numPartitions)

    val graph = sc.parallelize(Array(v1, v2, v3)).partitionBy(partitioner)

    val config = Params(rddPartitions = numPartitions)
    val rw = VCutRandomWalk(sc, config)
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
    val vType: Short = 0
    val pId = 0
    val v1 = (pId, (1, Array.empty[(Int, Int, Float)], vType))
    val v2 = (pId, (2, Array.empty[(Int, Int, Float)], vType))
    val v3 = (pId, (3, Array.empty[(Int, Int, Float)], vType))
    val numPartitions = 8

    val config = Params(rddPartitions = numPartitions)
    val rw = VCutRandomWalk(sc, config)
    val partitioner = rw.partitioner
    val graph = sc.parallelize(Array(v1, v2, v3)).partitionBy(partitioner)
    val rTable = rw.buildRoutingTable(graph)

    val w1 = (pId, (Array.empty[Int], Array.empty[(Int, Float)], false, vType))
    val w2 = (pId, (Array.empty[Int], Array.empty[(Int, Float)], false, vType))
    val w3 = (pId, (Array.empty[Int], Array.empty[(Int, Float)], false, vType))

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
    val vType: Short = 0
    val pId = 0
    val p1 = Array(1, 2, 3, 4)
    val w1 = (pId, (p1, Array.empty[(Int, Float)], false, vType))

    val p2 = Array(2, 5)
    val w2 = (pId, (p2, Array.empty[(Int, Float)], false, vType))

    val walkers = sc.parallelize(Array(w1, w2))

    val rw = VCutRandomWalk(sc, Params())

    val preparedWalkers = rw.prepareWalkersToTransfer(walkers).collect()
    assert(preparedWalkers.length == 2)
    val l1 = preparedWalkers.filter(_._2._1.head == p1.head)(0)._2
    ////    assert(map.get(last1) match {
    ////      case Some(w) => (w._2 sameElements p12) && (w._4 == op1) && (w._5 == wl1)
    ////      case None => false
    ////    })
    assert((l1._1 sameElements p1))
    //
    val l2 = preparedWalkers.filter(_._2._1.head == p2.head)(0)._2
    assert((l2._1 sameElements p2))
    //    assert(map.get(last2) match {
    //      case Some(w) => (w._2 sameElements p22) && (w._4 == op2) && (w._5 == wl2)
    //      case None => false
    //    })
  }

  test("filterUnfinishedWalkers") {
    val vType: Short = 0
    val pId = 0
    val p1 = (pId, (Array(1), Array.empty[(Int, Float)], true, vType))
    val p2 = (pId, (Array(2), Array.empty[(Int, Float)], false, vType))
    val p3 = (pId, (Array(3), Array.empty[(Int, Float)], false, vType))
    val walkers = sc.parallelize(Array(p1, p2, p3))

    val rw = VCutRandomWalk(sc, Params())

    val filteredWalkers = rw.filterUnfinishedWalkers(walkers).collect()

    assert(filteredWalkers.size == 2)
    assert(filteredWalkers.filter(_._2._1.head == p2._2._1.head).length == 1)
    assert(filteredWalkers.filter(_._2._1.head == p3._2._1.head).length == 1)

  }

  test("test 2nd order random walk undirected1") {
    // Undirected graph
    val rValue = 0.1f
    val wLength = 1
    val nextFloatGen = () => rValue
    val config = Params(input = karate, directed = false, walkLength =
      wLength, rddPartitions = 8, numWalks = 1)
    val rw = VCutRandomWalk(sc, config)
    val metaPath: Array[Short] = Array(0, 0)
    val graph = rw.loadGraph(hetero = false, sc.broadcast(metaPath))
    val paths = rw.randomWalk(graph, nextFloatGen, sc.broadcast(rw.metaPath))
    val rSampler = RandomSample(nextFloatGen)
    assert(paths.count() == rw.nVertices) // a path per vertex
    paths.collect().foreach { case (p: Array[Int]) =>
      val p2 = TestUtils.doSecondOrderRandomWalk(p(0), wLength, rSampler, 1.0f,
        1.0f)
      assert(p sameElements p2)
    }
  }

  test("test 2nd order random walk undirected2") {
    // Undirected graph
    val rValue = 0.1f
    val nextFloatGen = () => rValue
    val wLength = 50
    val config = Params(input = karate, directed = false, walkLength =
      wLength, rddPartitions = 8, numWalks = 1)
    val rw = VCutRandomWalk(sc, config)
    val metaPath: Array[Short] = Array(0, 0)
    val graph = rw.loadGraph(hetero = false, sc.broadcast(metaPath))
    val paths = rw.randomWalk(graph, nextFloatGen, sc.broadcast(rw.metaPath))
    assert(paths.count() == rw.nVertices) // a path per vertex
    val rSampler = RandomSample(nextFloatGen)
    paths.collect().foreach { case (p: Array[Int]) =>
      val p2 = TestUtils.doSecondOrderRandomWalk(p(0), wLength, rSampler, 1.0f,
        1.0f)
      assert(p sameElements p2)

    }
  }

  test("test 2nd order random walk undirected3") {
    // Undirected graph
    val wLength = 50
    val config = Params(input = karate, directed = false, walkLength =
      wLength, rddPartitions = 8, numWalks = 1)
    val rValue = 0.9f
    val nextFloatGen = () => rValue
    val rw = VCutRandomWalk(sc, config)
    val metaPath: Array[Short] = Array(0, 0)
    val graph = rw.loadGraph(hetero = false, sc.broadcast(metaPath))
    val paths = rw.randomWalk(graph, nextFloatGen, sc.broadcast(rw.metaPath))
    assert(paths.count() == rw.nVertices) // a path per vertex
    val rSampler = RandomSample(nextFloatGen)
    paths.collect().foreach { case (p: Array[Int]) =>
      val p2 = TestUtils.doSecondOrderRandomWalk(p(0), wLength, rSampler, 1.0f,
        1.0f)
      assert(p sameElements p2)
    }
  }

  test("test 2nd order random walk undirected4") {
    // Undirected graph
    val rValue = 0.1f
    val nextFloatGen = () => rValue
    val wLength = 50
    val config = Params(input = karate, directed = false, walkLength =
      wLength, rddPartitions = 8, numWalks = 1)
    val rw = VCutRandomWalk(sc, config)
    val metaPath: Array[Short] = Array(0, 0)
    val graph = rw.loadGraph(hetero = false, sc.broadcast(metaPath))
    val paths = rw.randomWalk(graph, nextFloatGen, sc.broadcast(rw.metaPath))
    assert(paths.count() == rw.nVertices) // a path per vertex
    val rSampler = RandomSample(nextFloatGen)
    paths.collect().foreach { case (p: Array[Int]) =>
      val p2 = TestUtils.doSecondOrderRandomWalk(p(0), wLength, rSampler, 1.0f,
        1.0f)
      assert(p sameElements p2)
    }
  }

  test("test 2nd order random walk directed1") {
    val wLength = 50
    val rValue = 0.9f
    val nextFloatGen = () => rValue

    // Directed Graph
    val config = Params(input = karate, directed = true, walkLength =
      wLength, rddPartitions = 8, numWalks = 1)
    val rw = VCutRandomWalk(sc, config)
    val metaPath: Array[Short] = Array(0, 0)
    val graph = rw.loadGraph(hetero = false, sc.broadcast(metaPath))
    val paths = rw.randomWalk(graph, nextFloatGen, sc.broadcast(rw.metaPath))
    assert(paths.count() == rw.nVertices) // a path per vertex
    val rSampler = RandomSample(nextFloatGen)
    paths.collect().foreach { case (p: Array[Int]) =>
      val p2 = TestUtils.doSecondOrderRandomWalk(p(0), wLength, rSampler, 1.0f,
        1.0f)
      assert(p sameElements p2)
    }
  }

  test("test 2nd order random walk directed2") {
    // Undirected graph
    val wLength = 50

    // Directed Graph
    val config = Params(input = karate, directed = true, walkLength =
      wLength, rddPartitions = 8, numWalks = 1)
    val rw = VCutRandomWalk(sc, config)
    val rValue = 0.1f
    val nextFloatGen = () => rValue
    val metaPath: Array[Short] = Array(0, 0)
    val graph = rw.loadGraph(hetero = false, sc.broadcast(metaPath))
    val paths = rw.randomWalk(graph, nextFloatGen, sc.broadcast(rw.metaPath))
    assert(paths.count() == rw.nVertices) // a path per vertex
    val rSampler = RandomSample(nextFloatGen)
    paths.collect().foreach { case (p: Array[Int]) =>
      val p2 = TestUtils.doSecondOrderRandomWalk(p(0), wLength, rSampler, 1.0f,
        1.0f)
      assert(p sameElements p2)
    }
  }


}
