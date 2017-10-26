package com.novacorp

import com.navercorp.Main.Params
import com.navercorp.RandomWalk
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.BeforeAndAfter

class RandomWalkTest extends org.scalatest.FunSuite with BeforeAndAfter {

  private val master = "local"
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
    RandomWalk.setup(sc, config)
    val graph = RandomWalk.loadGraph()
    assert(graph.edges.count() == 78)
    assert(graph.vertices.count() == 34)
  }

  test("load graph as undirected") {
    val config = Params(input = "./src/test/graph/karate.txt", directed = false)
    RandomWalk.setup(sc, config)
    val graph = RandomWalk.loadGraph()
    assert(graph.edges.count() == 156)
    assert(graph.vertices.count() == 34)
  }


}
