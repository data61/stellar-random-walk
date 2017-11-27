package au.csiro.data61.randomwalk.efficient.random

import org.apache.spark.graphx.Edge

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashMap}


/**
  *
  */

object GraphMap {

  private lazy val srcVertexMap: mutable.Map[Int, Int] = new HashMap[Int, Int]()
  private lazy val offsets: ArrayBuffer[Int] = new ArrayBuffer()
  private lazy val lengths: ArrayBuffer[Int] = new ArrayBuffer()
  private lazy val edges: ArrayBuffer[(Int, Float)] = new ArrayBuffer()
  private var indexCounter: Int = 0
  private var offsetCounter: Int = 0


  def addVertex(vId: Int, neighbors: Array[Edge[Float]]) = synchronized {
    srcVertexMap.put(vId, indexCounter)
    offsets.insert(indexCounter, offsetCounter)
    lengths.insert(indexCounter, neighbors.length)
    for (e <- 0 until neighbors.length) {
      edges.insert(offsetCounter, (neighbors(e).dstId.toInt, neighbors(e).attr))
      offsetCounter += 1
    }

    indexCounter += 1
  }

  def addVertex(vId: Int, neighbors: Array[(Int, Float)]): Unit = synchronized {
    srcVertexMap.get(vId) match {
      case None => {
        if (!neighbors.isEmpty) {
          srcVertexMap.put(vId, indexCounter)
          offsets.insert(indexCounter, offsetCounter)
          lengths.insert(indexCounter, neighbors.length)
          for (e <- neighbors) {
            edges.insert(offsetCounter, e)
            offsetCounter += 1
          }

          indexCounter += 1
        } else {
          this.addVertex(vId)
        }
      }
      case Some(value) => value
    }
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
    * structures that are initialy set by calling setUp function.
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
