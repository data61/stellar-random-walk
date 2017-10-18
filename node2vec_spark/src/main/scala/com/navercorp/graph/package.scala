package com.navercorp

import java.io.Serializable

package object graph {

  /**
    *
    * @param neighbors Array((dst, weight))
    * @param path      random walk path
    */
  case class NodeAttr(var neighbors: Array[(Long, Double)] = Array.empty[(Long, Double)],
                      var path: Array[Long] = Array.empty[Long]) extends Serializable

  /**
    *
    * @param dstNeighbors // neighbors of the destination node.
    * @param J            alias array
    * @param q            prob array
    */
  case class EdgeAttr(var dstNeighbors: Array[Long] = Array.empty[Long],
                      var J: Array[Int] = Array.empty[Int],
                      var q: Array[Double] = Array.empty[Double]) extends Serializable

}
