package au.csiro.data61

import au.csiro.data61.Main.Params
import org.apache.spark.graphx.{Edge, EdgeDirection, Graph}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.BeforeAndAfter

class RandomWalk2Test extends org.scalatest.FunSuite with BeforeAndAfter {

  private val master = "local[4]"
  private val appName = "rw-unit-test"
  private var sc: SparkContext = _

  before {
    val conf = new SparkConf().setMaster(master).setAppName(appName)
    sc = SparkContext.getOrCreate(conf)
  }

  after {
    if (sc != null) {
      sc.stop()
    }
  }

  test("load graph as directed") {
    val config = Params(input = "./src/test/graph/karate.txt", directed = true)
    val rw = RandomWalk2(sc, config)
    val paths = rw.loadGraph()
    val graph = rw.gMap
    assert(graph.value.numEdges == 78)
    assert(graph.value.numVertices == 34)
    assert(paths.count() == 34)
  }

  test("load graph as undirected") {
    val config = Params(input = "./src/test/graph/karate.txt", directed = false)
    val rw = RandomWalk2(sc, config)
    val paths = rw.loadGraph() // loadGraph(int)
    val graph = rw.gMap.value
    assert(graph.numEdges == 156)
    assert(graph.numVertices == 34)
    assert(paths.count() == 34)
  }

  ignore("the first step of Random Walk") {
    val config = Params(input = "./src/test/graph/testgraph.txt", directed = true)
    val rw = RandomWalk(sc, config)
    val graph = rw.loadGraph()
    val result = rw.doFirsStepOfRandomWalk(graph.collectEdges(EdgeDirection.Out), graph)
    val edges = graph.collectEdges(EdgeDirection.Out).collect()
    assert(result.count == graph.vertices.count())
    var curr = 2
    for (count <- 0 until 2) {
      val ((prev: Long, prevNeighbors: Option[Array[Edge[Double]]]), path: Array[Long]) =
        result.lookup(curr)(count)
      prevNeighbors match {
        case Some(edges) => {
          assert(prev == 1)
          assert(path sameElements Array(1L, 2L))
        }
        case None => {
          assert(prev == 2)
          assert(path sameElements Array(2L))
        }
        case _ => false
      }
    }
  }

  test("the first step of Random Walk2") {
    val config = Params(input = "./src/test/graph/testgraph.txt", directed = true)
    val rw = RandomWalk2(sc, config)
    val paths = rw.loadGraph()
    val result = rw.doFirsStepOfRandomWalk(paths)
    assert(result.count == paths.count())
    for (t <- result.collect()) {
      val p = t._2
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

  test("test 2nd order random walk2") {
    // Undirected graph
    var rValue = 0.1
    var wLength = 1
    var nextDoubleGen = () => rValue
    var config = Params(input = "./src/test/graph/karate.txt", directed = false, walkLength =
      wLength, rddPartitions = 8)
    var rw = RandomWalk2(sc, config)
    var graph = rw.loadGraph()
    var paths = rw.randomWalk(graph, nextDoubleGen)
    var rSampler = RandomSample(nextDoubleGen)
    assert(paths.count() == rw.gMap.value.numVertices) // a path per vertex
    var baseRw = RandomWalk(sc, config).loadGraph()
    paths.collect().foreach { case (t:Long, p: Array[Long]) =>
      val p2 = doSecondOrderRandomWalk(baseRw, p(0), wLength, rSampler)
      assert(p sameElements p2)
    }

    wLength = 50
    config = Params(input = "./src/test/graph/karate.txt", directed = false, walkLength =
      wLength, rddPartitions = 8)
    rw = RandomWalk2(sc, config)
    graph = rw.loadGraph()
    paths = rw.randomWalk(graph, nextDoubleGen)
    rSampler = RandomSample(nextDoubleGen)
    assert(paths.count() == rw.gMap.value.numVertices) // a path per vertex
    baseRw = RandomWalk(sc, config).loadGraph()
    paths.collect().foreach { case (t:Long, p: Array[Long]) =>
      val p2 = doSecondOrderRandomWalk(baseRw, p(0), wLength, rSampler)
      assert(p sameElements p2)
    }

    rValue = 0.9
    nextDoubleGen = () => rValue
    rw = RandomWalk2(sc, config)
    graph = rw.loadGraph()
    paths = rw.randomWalk(graph, nextDoubleGen)
    rSampler = RandomSample(nextDoubleGen)
    assert(paths.count() == rw.gMap.value.numVertices) // a path per vertex
    baseRw = RandomWalk(sc, config).loadGraph()
    paths.collect().foreach { case (t:Long, p: Array[Long]) =>
      val p2 = doSecondOrderRandomWalk(baseRw, p(0), wLength, rSampler)
      assert(p sameElements p2)
    }

    // Directed Graph
    config = Params(input = "./src/test/graph/karate.txt", directed = true, walkLength =
      wLength, rddPartitions = 8)
    rw = RandomWalk2(sc, config)
    graph = rw.loadGraph()
    paths = rw.randomWalk(graph, nextDoubleGen)
    rSampler = RandomSample(nextDoubleGen)
    assert(paths.count() == rw.gMap.value.numVertices) // a path per vertex
    baseRw = RandomWalk(sc, config).loadGraph()
    paths.collect().foreach { case (t:Long, p: Array[Long]) =>
      val p2 = doSecondOrderRandomWalk(baseRw, p(0), wLength, rSampler)
      assert(p sameElements p2)
    }

    rValue = 0.1
    nextDoubleGen = () => rValue
    graph = rw.loadGraph()
    paths = rw.randomWalk(graph, nextDoubleGen)
    rSampler = RandomSample(nextDoubleGen)
    assert(paths.count() == rw.gMap.value.numVertices) // a path per vertex
    baseRw = RandomWalk(sc, config).loadGraph()
    paths.collect().foreach { case (t:Long, p: Array[Long]) =>
      val p2 = doSecondOrderRandomWalk(baseRw, p(0), wLength, rSampler)
      assert(p sameElements p2)
    }
  }

  ignore("test 2nd order random walk") {
    // Undirected graph
    var rValue = 0.1
    var wLength = 1
    var nextDoubleGen = () => rValue
    var config = Params(input = "./src/test/graph/karate.txt", directed = false, walkLength =
      wLength, rddPartitions = 8)
    var rw = RandomWalk(sc, config)
    var graph = rw.loadGraph()
    var paths = rw.randomWalk(graph, nextDoubleGen)
    var rSampler = RandomSample(nextDoubleGen)
    assert(paths.count() == graph.vertices.count()) // a path per vertex
    paths.collect().foreach { (p: Array[Long]) =>
      val p2 = doSecondOrderRandomWalk(graph, p(0), wLength, rSampler)
      assert(p sameElements p2)
    }

    wLength = 50
    config = Params(input = "./src/test/graph/karate.txt", directed = false, walkLength =
      wLength, rddPartitions = 8)
    rw = RandomWalk(sc, config)
    graph = rw.loadGraph()
    paths = rw.randomWalk(graph, nextDoubleGen)
    rSampler = RandomSample(nextDoubleGen)
    assert(paths.count() == graph.vertices.count()) // a path per vertex
    paths.collect().foreach { (p: Array[Long]) =>
      val p2 = doSecondOrderRandomWalk(graph, p(0), wLength, rSampler)
      assert(p sameElements p2)
    }

    rValue = 0.9
    nextDoubleGen = () => rValue
    rw = RandomWalk(sc, config)
    graph = rw.loadGraph()
    paths = rw.randomWalk(graph, nextDoubleGen)
    rSampler = RandomSample(nextDoubleGen)
    assert(paths.count() == graph.vertices.count()) // a path per vertex
    paths.collect().foreach { (p: Array[Long]) =>
      val p2 = doSecondOrderRandomWalk(graph, p(0), wLength, rSampler)
      assert(p sameElements p2)
    }

    // Directed Graph
    config = Params(input = "./src/test/graph/karate.txt", directed = true, walkLength =
      wLength, rddPartitions = 8)
    rw = RandomWalk(sc, config)
    graph = rw.loadGraph()
    paths = rw.randomWalk(graph, nextDoubleGen)
    rSampler = RandomSample(nextDoubleGen)
    assert(paths.count() == graph.vertices.count()) // a path per vertex
    paths.collect().foreach { (p: Array[Long]) =>
      val p2 = doSecondOrderRandomWalk(graph, p(0), wLength, rSampler)
      assert(p sameElements p2)
    }

    rValue = 0.1
    nextDoubleGen = () => rValue
    graph = rw.loadGraph()
    paths = rw.randomWalk(graph, nextDoubleGen)
    rSampler = RandomSample(nextDoubleGen)
    assert(paths.count() == graph.vertices.count()) // a path per vertex
    paths.collect().foreach { (p: Array[Long]) =>
      val p2 = doSecondOrderRandomWalk(graph, p(0), wLength, rSampler)
      assert(p sameElements p2)
    }
  }

  private def doSecondOrderRandomWalk(graph: Graph[Array[Long], Double], src: Long,
                                      walkLength: Int, rSampler: RandomSample): Array[Long] = {
    val v2N = graph.collectEdges(EdgeDirection.Out)
    var path = Array(src)
    val neighbors = v2N.lookup(src)
    if (neighbors.length > 0) {
      path = rSampler.sample(v2N.lookup(src)(0)) match {
        case Some(e) => path ++ Array(e.dstId)
      }
    }
    else {
      return path
    }

    for (walkCount <- 0 until walkLength) {

      val curr = path.last
      val prev = path(path.length - 2)
      val currNeighbors = v2N.lookup(curr)
      if (currNeighbors.length > 0) {
        val prevNeighbors = Some(v2N.lookup(prev)(0))
        path = rSampler.secondOrderSample()(prev, prevNeighbors, currNeighbors(0)) match {
          case Some(e) => path ++ Array(e.dstId)
          case None => path
        }
      } else {
        return path
      }
    }

    path
  }

}
