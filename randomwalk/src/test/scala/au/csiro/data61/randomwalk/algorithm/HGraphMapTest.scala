package au.csiro.data61.randomwalk.algorithm

import org.scalatest.FunSuite

class HGraphMapTest extends FunSuite {

  test("test GraphMap data structure") {
    val e1 = Array((2, 1.0f))
    val e2 = Array((3, 1.0f))
    val e3 = Array((3, 1.0f))
    val e4 = Array((1, 1.0f))
    var v2N = Array((1, e1))
    val gMap = HGraphMap.initGraphMap(1).getGraphMap(0)
    gMap.addVertex(1, e1)
    gMap.addVertex(2)
    assert(gMap.getNumEdges == 1)
    assert(gMap.getNumVertices == 2)
    assertMap(v2N, gMap)

    gMap.reset
    v2N = Array((1, e1 ++ e2))
    gMap.addVertex(1, e1 ++ e2)
    gMap.addVertex(2)
    gMap.addVertex(3)
    assertMap(v2N, gMap)


    gMap.reset
    v2N = v2N ++ Array((2, e3 ++ e4))
    gMap.addVertex(2, e3 ++ e4)
    gMap.addVertex(1, e1 ++ e2)
    gMap.addVertex(3)
    assertMap(v2N, gMap)
  }

  private def assertMap(verticesToNeighbors: Array[(Int, Array[(Int, Float)])], gMap: GraphMap) = {
    for (v <- verticesToNeighbors) {
      var neighbors: Array[(Int, Float)] = Array()
      for (e <- v._2) {
        neighbors = neighbors ++ Array((e._1, e._2))
      }
      assert(gMap.getNeighbors(v._1) sameElements neighbors)
    }
  }

}
