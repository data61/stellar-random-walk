package au.csiro.data61.randomwalk.algorithm

object TestUtils {

  def doSecondOrderRandomWalk(src: Int, walkLength: Int, rSampler: RandomSample, p: Float,
                              q: Float): Array[Int] = {
    var path = Array(src)
    val neighbors = HGraphMap.getNeighbors(src, 0)
    if (neighbors.length > 0) {
      path = path ++ Array(rSampler.sample(neighbors)._1)
    }
    else {
      return path
    }

    for (_ <- 0 until walkLength) {

      val curr = path.last
      val prev = path(path.length - 2)
      val currNeighbors = HGraphMap.getNeighbors(curr, 0)
      if (currNeighbors.length > 0) {
        val prevNeighbors = HGraphMap.getNeighbors(prev, 0)
        path = path ++ Array(rSampler.secondOrderSample(p, q, prev, prevNeighbors, currNeighbors)
          ._1)
      } else {
        return path
      }
    }

    path
  }
}
