package com.navercorp.graph

import java.io.Serializable

/**
  *
  * @param dstNeighbors // neighbors of the destination node.
  * @param J alias array
  * @param q prob array
  */
class EdgeAttr(var dstNeighbors: Array[Long] = Array.empty[Long],
               var J: Array[Int] = Array.empty[Int],
               var q: Array[Double] = Array.empty[Double]) extends Serializable {
  private val serialVersionUID = 346406879093235518L
}