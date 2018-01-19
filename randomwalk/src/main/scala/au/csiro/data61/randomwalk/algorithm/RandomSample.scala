package au.csiro.data61.randomwalk.algorithm

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object RandomSample {


  /**
    *
    * @return
    */
  def sample(nextFloat: () => Float = Random.nextFloat)(edges: Array[(Int, Float)]): (Int, Float) = {

    val sum = edges.foldLeft(0.0) { case (w1, (_, w2)) => w1 + w2 }

    val p = nextFloat()
    var acc = 0.0
    for ((dstId, w) <- edges) {
      acc += w / sum
      if (acc >= p)
        return (dstId, w)
    }

    edges.head
  }

  def computeSecondOrderWeights(p: Float = 1.0f,
                                      q: Float = 1.0f,
                                      prevId: Int,
                                      prevNeighbors: Array[(Int, Float)],
                                      currNeighbors: Array[(Int, Float)]): Array[(Int, Float)] = {
    currNeighbors.map { case (dstId, w) =>
      var prob = w / q // Default is that there is no direct link between src and
      // dstNeighbor.
      if (dstId == prevId) prob = w / p // If the dstNeighbor is the src node.
      else {
        if (prevNeighbors.exists(_._1 == dstId)) prob = w
      }
      (dstId, prob)
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
  def secondOrderSample(nextFloat: () => Float = Random.nextFloat)
                             (p: Float = 1.0f,
                              q: Float = 1.0f,
                              prevId: Int,
                              prevNeighbors: Array[(Int, Float)],
                              currNeighbors: Array[(Int, Float)]): (Int, Float) = {
    val newCurrentNeighbors = computeSecondOrderWeights(p, q, prevId, prevNeighbors, currNeighbors)
    sample(nextFloat)(newCurrentNeighbors)
  }


  /**
    * Based on the paper: Vose, Michael D. "A linear algorithm for generating random numbers with
    * a given distribution." IEEE Transactions on software engineering 17.9 (1991): 972-975.
    */
  def initAlias(dstWeights: Array[(Int, Float)]): (Array[Int], Array[Float]) = {
    val small = new ArrayBuffer[Int]()
    val large = new ArrayBuffer[Int]()
    val sum: Float = dstWeights.foldLeft(0.0f) { case (w1, (_, w2)) => w1 + w2 }
    val k: Int = dstWeights.length
    val alias = Array.fill[Int](k)(0)
    val prob = Array.fill[Float](k)(0.0f)

    dstWeights.zipWithIndex.foreach { case ((_, w), index) =>
      prob(index) = k * w / sum
      if (prob(index) < 1.0f) {
        small.append(index)
      } else {
        large.append(index)
      }
    }

    while (small.nonEmpty && large.nonEmpty) {
      val s = small.remove(small.length - 1)
      val l = large.remove(large.length - 1)

      alias(s) = l
      prob(l) = prob(l) + prob(s) - 1.0f
      if (prob(l) < 1.0) small.append(l)
      else large.append(l)
    }

    (alias, prob)
  }

  def rand(nextFloat: () => Float = Random.nextFloat)(alias: Array[Int], prob: Array[Float]): Int
  = {
    val index = math.floor(nextFloat() * alias.length).toInt
    if (nextFloat() < prob(index)) index else alias(index)
  }

}
