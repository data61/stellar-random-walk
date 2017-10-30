package com.navercorp.graph2

import org.apache.spark.graphx.Edge

import scala.util.Random

case class RandomSample(nextDouble: () => Double = Random.nextDouble) extends Serializable {

  /**
    *
    * @param edges
    * @return
    */
  final def sample(edges: Array[Edge[Double]]): Option[Edge[Double]] = {
    val weights = edges.map(_.attr)
    resolveEdgeIndex(edges, sampleIndex(weights))
  }

  private final def resolveEdgeIndex(edges: Array[Edge[Double]], index: Int)
  : Option[Edge[Double]] = {
    index match {
      case -1 => None
      case _ => Some(edges(index))
    }
  }

  final def sampleIndex(weights: Array[Double]): Int = {
    val sum = weights.sum

    val p = nextDouble()
    val it = weights.iterator
    var acc = 0.0
    var i = 0
    while (it.hasNext) {
      val w = it.next
      acc += w / sum
      if (acc >= p)
        return i
      i = i + 1
    }

    return -1
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
                              q: Double = 1.0)(
                               prevId: Long,
                               prevNeighbors: Option[Array[Edge[Double]]],
                               currNeighbors: Array[Edge[Double]]): Option[Edge[Double]] = {

    val newWeights = computeSecondOrderWeights(p, q)(prevId, prevNeighbors, currNeighbors)
    resolveEdgeIndex(currNeighbors, sampleIndex(newWeights))
  }

  final def computeSecondOrderWeights(p: Double = 1.0,
                                      q: Double = 1.0)(
                                       prevId: Long,
                                       prevNeighbors: Option[Array[Edge[Double]]],
                                       currNeighbors: Array[Edge[Double]]): Array[Double] = {
    currNeighbors.map { case (e: Edge[Double]) =>
      var unnormProb = e.attr / q // Default is that there is no direct link between src and
      // dstNeighbor.
      if (e.dstId == prevId) unnormProb = e.attr / p // If the dstNeighbor is the src node.
      else {
        prevNeighbors match {
          case Some(edges) =>
            if ((edges.exists(_.dstId == e.dstId))) unnormProb = e.attr
        }
      }
      unnormProb
    } // If there is a
    // direct link from src to neighborDst. Note, that the weight of the direct link is always
    // considered, which does not necessarily is the shortest path.

  }

}
