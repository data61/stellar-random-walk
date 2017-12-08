package au.csiro.data61.randomwalk.vertexcut

import au.csiro.data61.randomwalk.RandomSample
import au.csiro.data61.randomwalk.common.Params
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.scalatest.BeforeAndAfter

import scala.collection.Map

class VCutRandomWalkTest extends org.scalatest.FunSuite with BeforeAndAfter {

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
    PartitionedGraphMap.reset
  }

  test("load graph as undirected") {
    val config = Params(input = "./src/test/graph/karate.txt", directed = false)
    val rw = VCutRandomWalk(sc, config)
    val paths = rw.loadGraph() // loadGraph(int)
    assert(rw.nEdges == 156)
    assert(rw.nVertices == 34)
    assert(paths.count() == 34)
    val vAcc = sc.longAccumulator("v")
    val eAcc = sc.longAccumulator("e")
    paths.coalesce(1).mapPartitions { iter =>
      vAcc.add(PartitionedGraphMap.getNumVertices)
      eAcc.add(PartitionedGraphMap.getNumEdges)
      iter
    }.first()
    assert(eAcc.sum == 156)
    assert(vAcc.sum == 34)
  }

  test("load graph as directed") {
    val config = Params(input = "./src/test/graph/karate.txt", directed = true)
    val rw = VCutRandomWalk(sc, config)
    val paths = rw.loadGraph()
    assert(rw.nEdges == 78)
    assert(rw.nVertices == 34)
    assert(paths.count() == 34)
    val vAcc = sc.longAccumulator("v")
    val eAcc = sc.longAccumulator("e")
    paths.coalesce(1).mapPartitions { iter =>
      vAcc.add(PartitionedGraphMap.getNumVertices)
      eAcc.add(PartitionedGraphMap.getNumEdges)
      iter
    }.first()
    assert(eAcc.sum == 78)
    assert(vAcc.sum == 34)
  }

  test("the first step of Random Walk") {
    val config = Params(input = "./src/test/graph/testgraph.txt", directed = true)
    val rw = VCutRandomWalk(sc, config)
    val paths = rw.loadGraph()
    val result = rw.doFirsStepOfRandomWalk(paths)
    assert(result.count == paths.count())
    for (t <- result.collect()) {
      val p = t._2._2
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
    val pId = 0
    val v1 = (pId, (1, Array.empty[(Int, Int, Float)]))
    val v2 = (pId, (2, Array.empty[(Int, Int, Float)]))
    val v3 = (pId, (3, Array.empty[(Int, Int, Float)]))
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
    val pId = 0
    val v1 = (pId, (1, Array.empty[(Int, Int, Float)]))
    val v2 = (pId, (2, Array.empty[(Int, Int, Float)]))
    val v3 = (pId, (3, Array.empty[(Int, Int, Float)]))
    val numPartitions = 8

    val config = Params(rddPartitions = numPartitions)
    val rw = VCutRandomWalk(sc, config)
    val partitioner = rw.partitioner
    val graph = sc.parallelize(Array(v1, v2, v3)).partitionBy(partitioner)
    val rTable = rw.buildRoutingTable(graph)

    val w1 = (pId, (1, Array.empty[Int], Array.empty[(Int, Float)], 1, 1))
    val w2 = (pId, (2, Array.empty[Int], Array.empty[(Int, Float)], 2, 2))
    val w3 = (pId, (3, Array.empty[Int], Array.empty[(Int, Float)], 3, 3))

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

    val pId = 0
    val p11 = Array(1, 2)
    val last1 = 4
    val p12 = Array(3, last1)
    val op1 = 1
    val wl1 = 5
    val w1 = (pId, (op1, p11 ++ p12, Array.empty[(Int, Float)], op1, wl1))
    val p21 = Array.empty[Int]
    val last2 = 5
    val p22 = Array(2, last2)
    val op2 = 2
    val wl2 = 2
    val w2 = (pId, (op2, p21 ++ p22, Array.empty[(Int, Float)], op2, wl2))

    val walkers = sc.parallelize(Array(w1, w2))

    val rw = VCutRandomWalk(sc, Params())

    val preparedWalkers = rw.prepareWalkersToTransfer(walkers).collect()
    assert(preparedWalkers.length == 2)
    val l1 = preparedWalkers.filter(_._2._1 == last1)(0)._2
    ////    assert(map.get(last1) match {
    ////      case Some(w) => (w._2 sameElements p12) && (w._4 == op1) && (w._5 == wl1)
    ////      case None => false
    ////    })
    assert((l1._2 sameElements p12) && (l1._4 == op1) && (l1._5 == wl1))
    //
    val l2 = preparedWalkers.filter(_._2._1 == last2)(0)._2
    assert((l2._2 sameElements p22) && (l2._4 == op2) && (l2._5 == wl2))
    //    assert(map.get(last2) match {
    //      case Some(w) => (w._2 sameElements p22) && (w._4 == op2) && (w._5 == wl2)
    //      case None => false
    //    })
  }

  test("test mergeNewPaths") {

    //merge with empty paths


    var paths = sc.emptyRDD[(Int, (Array[Int], Int))]
    val walkLength = sc.broadcast(4)
    val fP1 = Array(1, 2, 3)
    val oP1 = 1
    val p1 = (1, (fP1, Array.empty[(Int, Float)], oP1, walkLength.value))
    val p21 = Array(2)
    val p22 = Array(4, 5)
    val oP2 = 2
    val wl21 = 3
    var p2 = (2, (p21 ++ p22, Array.empty[(Int, Float)], oP2, wl21))
    var newPaths = sc.parallelize(Array(p1, p2))

    val rw = VCutRandomWalk(sc, Params())

    paths = rw.mergeNewPaths(paths, newPaths, walkLength)
    assert(paths.count() == 2)
    val merged: Map[Int, (Array[Int], Int)] = paths.collectAsMap()
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
    val p23 = Array(6)
    p2 = (2, (p22 ++ p23, Array.empty[(Int, Float)], oP2, wl22))
    newPaths = sc.parallelize(Array(p2))

    paths = rw.mergeNewPaths(paths, newPaths, walkLength)
    assert(paths.count() == 3)
    val mergedArray = paths.collect()

    mergedArray.foreach { case (origin: Int, (steps: Array[Int], wl: Int)) =>
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

    val w11 = (1, (Array.empty[Int], 1))
    val w12 = (w11._1, (Array(1, 2), 2))
    val w13 = (w11._1, (Array(3, 4), 4))
    val w21 = (2, (Array(2, 5), 1))
    val w22 = (w21._1, (Array(6, 7), 3))
    val w23 = (w21._1, (Array(8, 2), 5))
    val pieces = sc.parallelize(Array(w13, w11, w12, w22, w23, w21))

    val rw = VCutRandomWalk(sc, Params())

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
    val pId = 0
    val p1 = (pId, (1, Array.empty[Int], Array.empty[(Int, Float)], 1, walkLength.value))
    val p2 = (pId, (2, Array.empty[Int], Array.empty[(Int, Float)], 2, walkLength.value - 2))
    val p3 = (pId, (3, Array.empty[Int], Array.empty[(Int, Float)], 3, walkLength.value - 1))
    val walkers = sc.parallelize(Array(p1, p2, p3))

    val rw = VCutRandomWalk(sc, Params())

    val filteredWalkers = rw.filterUnfinishedWalkers(walkers, walkLength).collect()

    assert(filteredWalkers.size == 2)
    assert(filteredWalkers.filter(_._2._1 == p2._2._1).length == 1)
    assert(filteredWalkers.filter(_._2._1 == p3._2._1).length == 1)

  }

  test("test 2nd order random walk undirected1") {
    // Undirected graph
    val rValue = 0.1f
    val wLength = 1
    val nextFloatGen = () => rValue
    val config = Params(input = "./src/test/graph/karate.txt", directed = false, walkLength =
      wLength, rddPartitions = 8, numWalks = 1)
    val rw = VCutRandomWalk(sc, config)
    val graph = rw.loadGraph()
    val paths = rw.randomWalk(graph, nextFloatGen)
    val rSampler = RandomSample(nextFloatGen)
    assert(paths.count() == rw.nVertices) // a path per vertex
    paths.collect().foreach { case (p: List[Int]) =>
      val p2 = doSecondOrderRandomWalk(PartitionedGraphMap, p(0), wLength, rSampler, 1.0f, 1.0f)
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
    val rw = VCutRandomWalk(sc, config)
    val graph = rw.loadGraph()
    val paths = rw.randomWalk(graph, nextFloatGen)
    assert(paths.count() == rw.nVertices) // a path per vertex
    val rSampler = RandomSample(nextFloatGen)
    paths.collect().foreach { case (p: List[Int]) =>
      val p2 = doSecondOrderRandomWalk(PartitionedGraphMap, p(0), wLength, rSampler, 1.0f, 1.0f)
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
    val rw = VCutRandomWalk(sc, config)
    val graph = rw.loadGraph()
    val paths = rw.randomWalk(graph, nextFloatGen)
    assert(paths.count() == rw.nVertices) // a path per vertex
    val rSampler = RandomSample(nextFloatGen)
    paths.collect().foreach { case (p: List[Int]) =>
      val p2 = doSecondOrderRandomWalk(PartitionedGraphMap, p(0), wLength, rSampler, 1.0f, 1.0f)
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
    val rw = VCutRandomWalk(sc, config)
    val graph = rw.loadGraph()
    val paths = rw.randomWalk(graph, nextFloatGen)
    assert(paths.count() == rw.nVertices) // a path per vertex
    val rSampler = RandomSample(nextFloatGen)
    paths.collect().foreach { case (p: List[Int]) =>
      val p2 = doSecondOrderRandomWalk(PartitionedGraphMap, p(0), wLength, rSampler, 1.0f, 1.0f)
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
    val rw = VCutRandomWalk(sc, config)
    val graph = rw.loadGraph()
    val paths = rw.randomWalk(graph, nextFloatGen)
    assert(paths.count() == rw.nVertices) // a path per vertex
    val rSampler = RandomSample(nextFloatGen)
    paths.collect().foreach { case (p: List[Int]) =>
      val p2 = doSecondOrderRandomWalk(PartitionedGraphMap, p(0), wLength, rSampler, 1.0f, 1.0f)
      assert(p sameElements p2)
    }
  }

  test("test 2nd order random walk directed2") {
    // Undirected graph
    val wLength = 50

    // Directed Graph
    val config = Params(input = "./src/test/graph/karate.txt", directed = true, walkLength =
      wLength, rddPartitions = 8, numWalks = 1)
    val rw = VCutRandomWalk(sc, config)
    val rValue = 0.1f
    val nextFloatGen = () => rValue
    val graph = rw.loadGraph()
    val paths = rw.randomWalk(graph, nextFloatGen)
    assert(paths.count() == rw.nVertices) // a path per vertex
    val rSampler = RandomSample(nextFloatGen)
    paths.collect().foreach { case (p: List[Int]) =>
      val p2 = doSecondOrderRandomWalk(PartitionedGraphMap, p(0), wLength, rSampler, 1.0f, 1.0f)
      assert(p sameElements p2)
    }
  }

  private def doSecondOrderRandomWalk(gMap: PartitionedGraphMap.type, src: Int,
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
