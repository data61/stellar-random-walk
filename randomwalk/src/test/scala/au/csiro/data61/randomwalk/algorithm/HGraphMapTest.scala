package au.csiro.data61.randomwalk.algorithm

import org.scalatest.FunSuite

class HGraphMapTest extends FunSuite {

  val DEFAULT_TYPE:Short = 0

  test("test GraphMap data structure") {
    val e1 = Array((2, 1.0f))
    val e2 = Array((3, 1.0f))
    val e3 = Array((3, 1.0f))
    val e4 = Array((1, 1.0f))
    var v2N = Array((1, e1))
    HGraphMap.initGraphMap(1)
    HGraphMap.addVertex(DEFAULT_TYPE,1, e1)
    HGraphMap.addVertex(DEFAULT_TYPE,2)
    assert(HGraphMap.getNumEdges(DEFAULT_TYPE) == 1)
    assert(HGraphMap.getNumVertices(DEFAULT_TYPE) == 2)
    assertMap(v2N)

    HGraphMap.initGraphMap(1)
    v2N = Array((1, e1 ++ e2))
    HGraphMap.addVertex(DEFAULT_TYPE,1, e1 ++ e2)
    HGraphMap.addVertex(DEFAULT_TYPE,2)
    HGraphMap.addVertex(DEFAULT_TYPE,3)
    assertMap(v2N)


    HGraphMap.initGraphMap(1)
    v2N = v2N ++ Array((2, e3 ++ e4))
    HGraphMap.addVertex(DEFAULT_TYPE, 2, e3 ++ e4)
    HGraphMap.addVertex(DEFAULT_TYPE, 1, e1 ++ e2)
    HGraphMap.addVertex(DEFAULT_TYPE, 3)
    assertMap(v2N)
  }

  private def assertMap(verticesToNeighbors: Array[(Int, Array[(Int, Float)])]) = {
    for (v <- verticesToNeighbors) {
      var neighbors: Array[(Int, Float)] = Array()
      for (e <- v._2) {
        neighbors = neighbors ++ Array((e._1, e._2))
      }
      assert(HGraphMap.getNeighbors(v._1, DEFAULT_TYPE) sameElements neighbors)
    }
  }

}
