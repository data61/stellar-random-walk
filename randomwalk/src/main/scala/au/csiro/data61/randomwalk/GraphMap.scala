package au.csiro.data61.randomwalk

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashMap}


/**
  *
  */

trait GraphMap[T] {

  protected lazy val srcVertexMap: mutable.Map[Int, Int] = new HashMap[Int, Int]()
  protected lazy val offsets: ArrayBuffer[Int] = new ArrayBuffer()
  protected lazy val lengths: ArrayBuffer[Int] = new ArrayBuffer()
  protected lazy val edges: ArrayBuffer[(Int, Float)] = new ArrayBuffer()
  protected var indexCounter: Int = 0
  protected var offsetCounter: Int = 0
  protected var firstGet: Boolean = true

  def addVertex(vId: Int, neighbors: Array[T])

  def getGraphStatsOnlyOnce: (Int, Int) = synchronized {
    if (firstGet) {
      firstGet = false
      (srcVertexMap.size, offsetCounter)
    }
    else
      (0,0)
  }

  def resetGetters {
    firstGet = true
  }

  def addVertex(vId: Int): Unit = synchronized {
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
    * structures that are initially set by calling setUp function.
    */
  def reset {
    indexCounter = 0
    offsetCounter = 0
    srcVertexMap.clear()
    offsets.clear()
    lengths.clear()
    edges.clear()
  }

  def getNeighbors(vid: Int): Array[(Int, Float)] = {
    srcVertexMap.get(vid) match {
      case Some(index) =>
        if (index == -1) {
          return Array.empty[(Int, Float)]
        }
        val offset = offsets(index)
        val length = lengths(index)
        edges.slice(offset, offset + length).toArray
      case None => null
    }
  }
}
