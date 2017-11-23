package au.csiro.data61.randomwalk.efficient

import scala.util.Random

case class RandomSample(nextDouble: () => Double = Random.nextDouble) extends Serializable {


  /**
    *
    * @return
    */
  final def sample(edges: Array[(Long, Double)]): (Long, Double) = {

    val sum = edges.foldLeft(0.0) { case (w1, (_, w2)) => w1 + w2 }

    val p = nextDouble()
    var acc = 0.0
    for ((dstId, w) <- edges) {
      acc += w / sum
      if (acc >= p)
        return (dstId, w)
    }

    edges.head
  }

  final def computeSecondOrderWeights(p: Double = 1.0,
                                      q: Double = 1.0,
                                      prevId: Long,
                                      prevNeighbors: Array[(Long, Double)],
                                      currNeighbors: Array[(Long, Double)]): Array[(Long, Double)
    ] = {
    currNeighbors.map { case (dstId, w) =>
      var unnormProb = w / q // Default is that there is no direct link between src and
      // dstNeighbor.
      if (dstId == prevId) unnormProb = w / p // If the dstNeighbor is the src node.
      else {
        if (prevNeighbors.exists(_._1 == dstId)) unnormProb = w
      }
      (dstId, unnormProb)
    } // If there is a
    // direct link from src to neighborDst. Note, that the weight of the direct link is always
    // considered, which does not necessarily is the shortest path.
  }

  /**
    *
    * @param p
    * @param q
    * @param prevId
    * @param prevNeighbors
    * @param currNeighbors
    * @return
    */
  final def secondOrderSample(p: Double = 1.0,
                              q: Double = 1.0,
                              prevId: Long,
                              prevNeighbors: Array[(Long, Double)],
                              currNeighbors: Array[(Long, Double)]): (Long, Double) = {
    val newCurrentNeighbors = computeSecondOrderWeights(p, q, prevId, prevNeighbors, currNeighbors)
    sample(newCurrentNeighbors)
  }
}
