package au.csiro.data61.randomwalk.efficient


import org.apache.spark.graphx.Edge

import scala.collection.mutable
import scala.collection.mutable.HashMap


/**
  *
  */

object GraphMap {

  private var numVertices: Int = 0
  private var numDeadEnds: Int = 0
  private var numEdges: Int = 0

  private lazy val srcVertexMap: mutable.Map[Long, Int] = new HashMap[Long, Int]()
  private lazy val offsets: Array[Int] = new Array(numVertices - numDeadEnds)
  private lazy val lengths: Array[Int] = new Array(numVertices - numDeadEnds)
  private lazy val edges: Array[(Long, Double)] = new Array(numEdges)
  private var indexCounter: Int = 0
  private var offsetCounter: Int = 0

  /**
    * It only affects before calling addVertex for the first time. The size cannot grow after that.
    *
    * @param numVertices
    * @param numDeadEnds
    * @param numEdges
    */
  def setUp(numVertices: Int, numDeadEnds: Int, numEdges: Int) = {
    this.numVertices = numVertices
    this.numDeadEnds = numDeadEnds
    this.numEdges = numEdges

  }

  def addVertex(vId: Long, neighbors: Array[Edge[Double]]) = synchronized {
    srcVertexMap.put(vId, indexCounter)
    offsets(indexCounter) = offsetCounter
    lengths(indexCounter) = neighbors.length
    for (e <- 0 until neighbors.length) {
      edges(offsetCounter) = (neighbors(e).dstId, neighbors(e).attr)
      offsetCounter += 1
    }

    indexCounter += 1
  }

  def addVertex(vId: Long, neighbors: Array[(Long, Double)]): Unit = synchronized {
    if (!neighbors.isEmpty) {
      srcVertexMap.put(vId, indexCounter)
      offsets(indexCounter) = offsetCounter
      lengths(indexCounter) = neighbors.length
      for (e <- neighbors) {
        edges(offsetCounter) = e
        offsetCounter += 1
      }

      indexCounter += 1
    } else {
      this.addVertex(vId)
    }
  }

  def addVertex(vId: Long) {
    srcVertexMap.put(vId, -1)
  }

  def getNumVertices: Int = {
    srcVertexMap.size
  }

  def getNumEdges: Int = {
    offsetCounter
  }

  /**
    * The reset is mainly for the unit test purpose. It does not reset the size of data
    * structures that are initialy set by calling setUp function.
    */
  def reset {
    numVertices = 0
    numEdges = 0
    numDeadEnds = 0
    indexCounter = 0
    offsetCounter = 0
    srcVertexMap.clear()
  }

  def getNeighbors(vid: Long): Array[(Long, Double)] = {
    srcVertexMap.get(vid) match {
      case Some(index) =>
        if (index == -1) {
          return Array.empty[(Long, Double)]
        }
        val offset = offsets(index)
        val length = lengths(index)
        edges.slice(offset, offset + length)
      case None => null
    }
  }
}
