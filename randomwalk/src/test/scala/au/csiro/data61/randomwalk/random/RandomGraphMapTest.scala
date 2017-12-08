package au.csiro.data61.randomwalk.random

import org.scalatest.FunSuite

class RandomGraphMapTest extends FunSuite {

  test("test GraphMap data structure") {
    val e1 = Array((2, 1.0f))
    val e2 = Array((3, 1.0f))
    val e3 = Array((3, 1.0f))
    val e4 = Array((1, 1.0f))
    var v2N = Array((1, e1))
    RandomGraphMap.addVertex(1, e1)
    RandomGraphMap.addVertex(2)
    assert(RandomGraphMap.getNumEdges == 1)
    assert(RandomGraphMap.getNumVertices == 2)
    assertMap(v2N, RandomGraphMap)

    RandomGraphMap.reset
    v2N = Array((1, e1 ++ e2))
    RandomGraphMap.addVertex(1, e1 ++ e2)
    RandomGraphMap.addVertex(2)
    RandomGraphMap.addVertex(3)
    assertMap(v2N, RandomGraphMap)


    RandomGraphMap.reset
    v2N = v2N ++ Array((2, e3 ++ e4))
    RandomGraphMap.addVertex(2, e3 ++ e4)
    RandomGraphMap.addVertex(1, e1 ++ e2)
    RandomGraphMap.addVertex(3)
    assertMap(v2N, RandomGraphMap)
  }

  private def assertMap(verticesToNeighbors: Array[(Int, Array[(Int,Float)])], gMap: RandomGraphMap
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
