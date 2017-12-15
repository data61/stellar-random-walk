package au.csiro.data61.randomwalk.algorithm

import org.scalatest.FunSuite

class GraphMapTest extends FunSuite {

  test("test GraphMap data structure") {
    val e1 = Array((2, 1.0f))
    val e2 = Array((3, 1.0f))
    val e3 = Array((3, 1.0f))
    val e4 = Array((1, 1.0f))
    var v2N = Array((1, e1))
    GraphMap.addVertex(1, e1)
    GraphMap.addVertex(2)
    assert(GraphMap.getNumEdges == 1)
    assert(GraphMap.getNumVertices == 2)
    assertMap(v2N, GraphMap)

    GraphMap.reset
    v2N = Array((1, e1 ++ e2))
    GraphMap.addVertex(1, e1 ++ e2)
    GraphMap.addVertex(2)
    GraphMap.addVertex(3)
    assertMap(v2N, GraphMap)


    GraphMap.reset
    v2N = v2N ++ Array((2, e3 ++ e4))
    GraphMap.addVertex(2, e3 ++ e4)
    GraphMap.addVertex(1, e1 ++ e2)
    GraphMap.addVertex(3)
    assertMap(v2N, GraphMap)
  }

  private def assertMap(verticesToNeighbors: Array[(Int, Array[(Int,Float)])], gMap: GraphMap
    .type) = {
    for (v <- verticesToNeighbors) {
      var neighbors: Array[(Int, Float)] = Array()
      for (e <- v._2) {
        neighbors = neighbors ++ Array((e._1, e._2))
      }
      assert(gMap.getNeighbors(v._1) sameElements neighbors)
    }
  }

}
