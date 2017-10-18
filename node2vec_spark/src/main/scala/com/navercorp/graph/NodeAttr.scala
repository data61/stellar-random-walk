package com.navercorp.graph

import java.io.Serializable

/**
  *
  * @param neighbors Array((dst, weight))
  * @param path random walk path
  */
class NodeAttr(var neighbors: Array[(Long, Double)] = Array.empty[(Long, Double)],
               var path: Array[Long] = Array.empty[Long]) extends Serializable {
  private val serialVersionUID = - 3839839695824018974L
}
