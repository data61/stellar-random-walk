package com.navercorp.graph2

import org.apache.spark.graphx.Edge

object RandomSample extends Serializable {

  // TODO define seed
  // TODO Unit test

  /**
    *
    * @param edges
    * @return
    */
  final def sample(edges: Array[Edge[Double]]): Option[Edge[Double]] = {
    val sum = edges.map(_.attr).sum

    val p = scala.util.Random.nextDouble
    val it = edges.iterator
    var acc = 0.0
    while (it.hasNext) {
      val e = it.next
      acc += e.attr / sum
      if (acc >= p)
        return Some(e)
    }

    None
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
                               prevNeighbors: Array[Edge[Double]],
                               currNeighbors: Array[Edge[Double]]): Option[Edge[Double]] = {
    val neighbors_ = currNeighbors.map { case (e: Edge[Double]) =>
      var unnormProb = e.attr / q // Default is that there is no direct link between src and
      // dstNeighbor.
      if (e.dstId == prevId) unnormProb = e.attr / p // If the dstNeighbor is the src node.
      else if (prevNeighbors.exists(_.dstId == e.dstId)) unnormProb = e.attr // If there is a
      // direct link from src to neighborDst. Note, that the weight of the direct link is always
      // considered, which does not necessarily is the shortest path.

      Edge(e.srcId, e.dstId, unnormProb)
    }
    sample(neighbors_)
  }
}
