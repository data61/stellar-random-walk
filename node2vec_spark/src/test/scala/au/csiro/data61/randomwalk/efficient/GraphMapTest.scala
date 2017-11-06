package au.csiro.data61.randomwalk.efficient

import org.apache.spark.graphx.Edge
import org.scalatest.FunSuite

class GraphMapTest extends FunSuite {

  test("test GraphMap data structure") {
    val e1 = Edge(1, 2, 1.0)
    var v2N = Array((1L, Array(e1)))
    var gMap = GraphMap()
    gMap.setUp(v2N, 1)
    assert(gMap.numEdges == 1)
    assert(gMap.numVertices == 1)
    assertMap(v2N, gMap)

    val e2 = Edge(1, 3, 1.0)
    v2N = Array((1L, Array(e1, e2)))
    gMap = GraphMap()
    gMap.setUp(v2N, 2)
    assertMap(v2N, gMap)

    val e3 = Edge(2, 3, 1.0)
    val e4 = Edge(2, 1, 1.0)
    v2N = v2N ++ Array((2L, Array(e3, e4)))
    gMap = GraphMap()
    gMap.setUp(v2N, 4)
    assertMap(v2N, gMap)
  }

  private def assertMap(verticesToNeighbors: Array[(Long, Array[Edge[Double]])], gMap: GraphMap) = {
    for (v <- verticesToNeighbors) {
      var neighbors: Array[(Long, Double)] = Array()
      for (e <- v._2) {
        neighbors = neighbors ++ Array((e.dstId, e.attr))
      }
      assert(gMap.getNeighbors(v._1) sameElements neighbors)
    }
  }

}
