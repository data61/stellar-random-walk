package au.csiro.data61.randomwalk.algorithm

import au.csiro.data61.randomwalk.common.Params
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.scalatest.BeforeAndAfter


class UniformRandomWalkTest extends org.scalatest.FunSuite with BeforeAndAfter {

  private val karate = "./randomwalk/src/test/resources/karate.txt"
  private val karateNodeTypes = "./randomwalk/src/test/resources/karate-node-types.txt"
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
    HGraphMap.initGraphMap(3)
    val conf = new SparkConf().setMaster(master).setAppName(appName)
    sc = SparkContext.getOrCreate(conf)
  }

  after {
    if (sc != null) {
      sc.stop()
    }
    HGraphMap.reset
  }

  test("metapath input") {
    val types = Array("0", "1", "2")
    val defaultMp = Array(types(0).toShort, types(0).toShort)
    assert(defaultMp sameElements UniformRandomWalk(sc, Params()).metaPath)
    val mp = Array(types(0).toShort, types(1).toShort, types(2).toShort)
    assert(mp sameElements UniformRandomWalk(sc, Params(metaPath = "0 1 2")).metaPath)
  }

  test("load node types") {
    val config = Params(vTypeInput = karateNodeTypes, directed = false)
    val rw = UniformRandomWalk(sc, config)
    val types = rw.loadNodeTypes().collect()
    assert(types.forall { case (vId: Int, t: Short) => (vId - 1) % 3 == t })
  }

  test("load heterogeneous graph 1") {
    val config = Params(input = karate, vTypeInput = karateNodeTypes, directed = false)
    val rw = UniformRandomWalk(sc, config)
    val g = rw.loadHeteroGraph()
    val gArray = g.collect()
    assert(gArray.forall { case (src, (edgeTypes, srcType)) =>
      ((src - 1) % 3 == srcType) && edgeTypes.forall { case (neighbors, dstType) =>
        neighbors.forall { case (dst, _) => (dst - 1) % 3 == dstType }
      }
    })

    val vSize = gArray.length
    val eSize = gArray.foldLeft(0) {
      _ + _._2._1.foldLeft(0)(_ + _._1.length)
    }
    assert(vSize == 34)
    assert(eSize == 156)
  }

  test("load heterogeneous graph 2") {
    val config = Params(input = karate, vTypeInput = karateNodeTypes, vTypeSize = 3, directed =
      false)
    val rw = UniformRandomWalk(sc, config)
    val metaPath: Array[Short] = Array(0, 0)
    val paths = rw.loadGraph(hetero = true, sc.broadcast(metaPath))

    assert(rw.nEdges == 156)
    assert(rw.nVertices == 34)
    assert(paths.count() == 12) // only paths starting from vertex-types 0.
    assert(paths.collect().forall(p => (p._1 - 1) % 3 == metaPath(0)))

  }

  test("append node types") {
    var bcDirected = sc.broadcast(false)
    val config = Params()
    var rw = UniformRandomWalk(sc, config)
    // (dst, (src, w))
    val v1 = (1, (2, 1.0f))
    val v2 = (2, (1, 1.0f))
    val v3 = (3, (4, 1.0f))
    val v4 = (4, (3, 1.0f))
    val t1 = (1, 1.toShort)
    val t2 = (2, 2.toShort)
    val t3 = (3, 3.toShort)
    val t4 = (4, 1.toShort)
    var edges = sc.parallelize(Array(v1, v2, v3, v4))
    val vTypes = sc.parallelize(Array(t1, t2, t3, t4))
    var ap = rw.appendNodeTypes(edges, vTypes, bcDirected).collectAsMap()
    // ((src, dstType), (dst, weight))
    val keys = Array((v1._1, t2._2), (v2._1, t1._2), (v4._1, t3._2), (v3._1, t4._2))
    val values = Array(t2._1, t1._1, t3._1, t4._1)

    for ((k, v) <- (keys zip values)) {
      ap.get(k) match {
        case None => assert(false)
        case Some(edge) => assert(edge(0)._1 == v)
      }
    }

    bcDirected = sc.broadcast(true)
    rw = UniformRandomWalk(sc, config)

    edges = sc.parallelize(Array(v1))
    ap = rw.appendNodeTypes(edges, vTypes, bcDirected).collectAsMap()
    ap.get((v1._1, t1._2)) match {
      case None => assert(false)
      case Some(edge) => assert(edge.isEmpty)
    }
    ap.get((v2._1, t1._2)) match {
      case None => assert(false)
      case Some(edge) => assert(edge(0)._1 == t1._1)
    }
  }

  test("load graph as undirected") {
    val config = Params(input = karate, directed = false)
    val rw = UniformRandomWalk(sc, config)
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
    val rw = UniformRandomWalk(sc, config)
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
    val rw = UniformRandomWalk(sc, config)
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
    val t: Short = 0
    val v1 = (1, (Array((Array.empty[(Int, Float)], t)), t))
    val v2 = (2, (Array((Array.empty[(Int, Float)], t)), t))
    val v3 = (3, (Array((Array.empty[(Int, Float)], t)), t))
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
    val t: Short = 0
    val v1 = (1, (Array((Array.empty[(Int, Float)], t)), t))
    val v2 = (2, (Array((Array.empty[(Int, Float)], t)), t))
    val v3 = (3, (Array((Array.empty[(Int, Float)], t)), t))
    val numPartitions = 8

    val config = Params(rddPartitions = numPartitions)
    val rw = UniformRandomWalk(sc, config)
    val partitioner = rw.partitioner
    val graph = sc.parallelize(Array(v1, v2, v3)).partitionBy(partitioner)
    val rTable = rw.buildRoutingTable(graph)

    val w1 = (1, (Array.empty[Int], Array.empty[(Int, Float)], false, t))
    val w2 = (2, (Array.empty[Int], Array.empty[(Int, Float)], false, t))
    val w3 = (3, (Array.empty[Int], Array.empty[(Int, Float)], false, t))

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
    val p1 = Array(1, 2, 3, 4)
    val op1 = 1
    val w1 = (op1, (p1, Array.empty[(Int, Float)], false, vType))
    val p2 = Array(2, 5)
    val op2 = 2
    val w2 = (op2, (p2, Array.empty[(Int, Float)], false, vType))

    val walkers = sc.parallelize(Array(w1, w2))

    val rw = UniformRandomWalk(sc, Params())

    val preparedWalkers = rw.prepareWalkersToTransfer(walkers).collect()
    assert(preparedWalkers.length == 2)
    val l1 = preparedWalkers.filter(_._2._1.head == p1.head)(0)._2

    assert((l1._1 sameElements p1))
    //
    val l2 = preparedWalkers.filter(_._2._1.head == p2.head)(0)._2
    assert((l2._1 sameElements p2))
  }

  test("filterUnfinishedWalkers") {
    val vType: Short = 0
    val p1 = (1, (Array.empty[Int], Array.empty[(Int, Float)], true, vType))
    val p2 = (2, (Array.empty[Int], Array.empty[(Int, Float)], false, vType))
    val p3 = (3, (Array.empty[Int], Array.empty[(Int, Float)], false, vType))
    val walkers = sc.parallelize(Array(p1, p2, p3))

    val rw = UniformRandomWalk(sc, Params())

    val filteredWalkers = rw.filterUnfinishedWalkers(walkers).collect()

    assert(filteredWalkers.size == 2)
    assert(filteredWalkers.filter(_._1 == p2._1).length == 1)
    assert(filteredWalkers.filter(_._1 == p3._1).length == 1)

  }

  test("heterogeneous walk") {
    val typeSize: Short = 3
    var config: Params = Params(input = karate, vTypeInput = karateNodeTypes, vTypeSize = typeSize,
      metaPath = "0 1 2 1", directed = false, numWalks = 1)
    var rw = UniformRandomWalk(sc, config)
    var paths = rw.execute()
    assert(paths.count() == 12) // only paths starting from vertex-type 0.
    var mpLength = rw.metaPath.length
    var metaPath = rw.metaPath
    assert(paths.collect().forall { p =>
      p.view.zipWithIndex.forall { case (v, i) =>
        metaPath(i % mpLength) == (v - 1) % typeSize
      }
    })

    config = Params(input = karate, vTypeInput = karateNodeTypes, vTypeSize = typeSize,
      metaPath = "0", directed = false, numWalks = 1)
    rw = UniformRandomWalk(sc, config)
    paths = rw.execute()
    assert(paths.count() == 12) // only paths starting from vertex-type 0.
    mpLength = rw.metaPath.length
    metaPath = rw.metaPath
    assert(paths.collect().forall { p =>
      p.view.zipWithIndex.forall { case (v, i) =>
        metaPath(i % mpLength) == (v - 1) % typeSize
      }
    })
  }


  test("test 2nd order random walk undirected1") {
    // Undirected graph
    val rValue = 0.1f
    val wLength = 1
    val nextFloatGen = () => rValue
    val config = Params(input = karate, directed = false, walkLength =
      wLength, rddPartitions = 8, numWalks = 1)
    val rw = UniformRandomWalk(sc, config)
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
    val rw = UniformRandomWalk(sc, config)
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
    val rw = UniformRandomWalk(sc, config)
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
    val rw = UniformRandomWalk(sc, config)
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
    val rw = UniformRandomWalk(sc, config)
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
    val rw = UniformRandomWalk(sc, config)
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
