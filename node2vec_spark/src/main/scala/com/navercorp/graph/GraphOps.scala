package com.navercorp.graph

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{EdgeTriplet, Graph, Edge, _}
import org.apache.spark.rdd.RDD
import com.navercorp.Main

object GraphOps {
  var context: SparkContext = _
  var config: Main.Params = _

  def setup(context: SparkContext, param: Main.Params): this.type = {
    this.context = context
    this.config = param

    this
  }

  /* This is an implementation of the Alias Sampling according to the paper:
   "The Linear Algorithm for generaing random numbers with a given distribution" by Micheal D.
   Vose. */
  // Implementation of Init algorithm
  // Returns (alias, prob)
  def setupAlias(nodeWeights: Array[(Long, Double)]): (Array[Int], Array[Double]) = {
    val K = nodeWeights.length // number of items
    val J = Array.fill(K)(0) // alias array
    val q = Array.fill(K)(0.0) // prob array

    val smaller = new ArrayBuffer[Int]() // Edges with w/sum < 1/k
    val larger = new ArrayBuffer[Int]() // Edges with w/sum >= 1/k
    // all edges go to this array.

    //compute sum of outbound edge weights
    //aO(k)
    val sum = nodeWeights.map(_._2).sum
    //2*tO(K) ; i is the new id of the (nodeId, weight) tuples from [0,k)
    nodeWeights.zipWithIndex.foreach { case ((nodeId, weight), i) =>
      // q is always 1.0 in an unweighted graph for all the neighbors
      q(i) = K * weight / sum
      if (q(i) < 1.0) {
        smaller.append(i)
      } else {
        larger.append(i)
      }
    }

    // O(K)
    while (smaller.nonEmpty && larger.nonEmpty) {
      val small = smaller.remove(smaller.length - 1)
      val large = larger.remove(larger.length - 1)

      J(small) = large
      q(large) = q(large) + q(small) - 1.0
      if (q(large) < 1.0) smaller.append(large)
      else larger.append(large)
    }

    (J, q)
  }

  /**
    *
    * @param p
    * @param q
    * @param srcId
    * @param srcNeighbors
    * @param dstNeighbors
    * @return alias and prob arrays
    */
  def setupEdgeAlias(p: Double = 1.0,
                     q: Double = 1.0)(srcId: Long,
                                      srcNeighbors: Array[(Long, Double)],
                                      dstNeighbors: Array[(Long, Double)]): (Array[Int],
    Array[Double]) = {
    val neighbors_ = dstNeighbors.map { case (dstNeighborId, weight) =>
      var unnormProb = weight / q // Default is that there is no direct link between src and
      // dstNeighbor.
      if (srcId == dstNeighborId) unnormProb = weight / p // If the dstNeighbor is the src node.
      else if (srcNeighbors.exists(_._1 == dstNeighborId)) unnormProb = weight // If there is a
      // direct link from src to neighborDst. Note, that the weight of the direct link is always
      // considered, which does not necessarily is the shortest path.

      (dstNeighborId, unnormProb)
    }

    setupAlias(neighbors_) // creating alias and prob arrays for destNeighbors
  }

  // Implementation of Rand algorithm
  // q is array of "prob"
  // J is array of "alias"
  // O(1)
  def drawAlias(J: Array[Int], q: Array[Double]): Int = {
    val K = J.length
    val kk = math.floor(math.random * K).toInt // Choose a random number between [0, K)

    /* This is slightly different from the original algorithm that is r = math.random that r is
    the random number computed previously and is if (r-kk)<= q(kk) */
    if (math.random < q(kk)) kk // return the number itself
    else J(kk) // otherwise return alias of that number
  }

  def initTransitionProb(indexedNodes: RDD[(VertexId, NodeAttr)], indexedEdges:
  RDD[Edge[EdgeAttr]]) = {
    // Set p and q values
    val bcP = context.broadcast(config.p)
    val bcQ = context.broadcast(config.q)

    // Creates an object of the graphx Graph. This makes the graph to be stored with the GraphX
    // data structure. Right?
    // Since the there is no previous vertex, therefore, in the first round it uses the 1st order
    // random walk for initialization.
    val graph = Graph(indexedNodes, indexedEdges).mapVertices[NodeAttr] { case (vertexId,
    nodeAttr) =>
      /* For each vertex create alias and prob arrays. This is done by giving the set of items.*/
      val (j, q) = GraphOps.setupAlias(nodeAttr.neighbors)
      val nextNodeIndex = GraphOps.drawAlias(j, q) // selects a random node from the neighbors
      // according to their weights
      nodeAttr.path = Array(vertexId, nodeAttr.neighbors(nextNodeIndex)._1)
      nodeAttr
    }.mapTriplets { edgeTriplet: EdgeTriplet[NodeAttr, EdgeAttr] => // EdgeTriplet includes
      // (srcAttr, dstAttr, edgeAttr)
      val (j, q) = GraphOps.setupEdgeAlias(bcP.value, bcQ.value)(edgeTriplet.srcId,
        edgeTriplet.srcAttr.neighbors,
        edgeTriplet.dstAttr.neighbors)

      edgeTriplet.attr.J = j // alias array
      edgeTriplet.attr.q = q // prob array
      edgeTriplet.attr.dstNeighbors = edgeTriplet.dstAttr.neighbors.map(_._1) // Removing the weights from dst attribute?

      edgeTriplet.attr
    }.cache

    graph
  }

}
