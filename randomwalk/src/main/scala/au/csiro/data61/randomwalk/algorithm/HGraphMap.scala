package au.csiro.data61.randomwalk.algorithm

import scala.collection.mutable

object HGraphMap {
  private var firstGet: Boolean = true
  private var initialized = false
  private val vertices = new mutable.HashSet[Int]()

  var hGraph: Array[GraphMap] = null

  def initGraphMap(numVertexTypes: Int) = synchronized {
    if (!initialized) {
      vertices.clear()
      hGraph = new Array[GraphMap](numVertexTypes).map(_ => GraphMap())
      initialized = true
    }
  }

  def addVertex(dstType: Short, vId: Int, neighbors: Array[(Int, Float)]): Unit = synchronized {
    vertices.add(vId)
    hGraph(dstType).addVertex(vId, neighbors)
  }

  def addVertex(dstType: Short, vId: Int, neighbors: Array[(Int, Int, Float)]): Unit
  = synchronized {
    vertices.add(vId)
    hGraph(dstType).addVertex(vId, neighbors)
  }

  def addVertex(vType: Short, vId: Int): Unit = synchronized {
    vertices.add(vId)
    hGraph(vType).addVertex(vId)
  }

  def getPartition(vType: Short, vId: Int): Option[Int] = {
    hGraph(vType).getPartition(vId)
  }

  //  def getGraphMap(vType: Short): GraphMap = {
  //    hGraph(vType)
  //  }

  def getNeighbors(vId: Int, vType: Short): Array[(Int, Float)] = {
    hGraph(vType).getNeighbors(vId)
  }

  def getNumEdges(vType: Short): Int = {
    hGraph(vType).getNumEdges
  }

  def getNumVertices(vType: Short): Int = {
    hGraph(vType).getNumVertices
  }

  def getGraphStatsOnlyOnce: (Int, Int) = synchronized {
    if (firstGet) {
      firstGet = false
      val n: Int = hGraph.foldLeft(0)(_ + _.getNumVertices)
      val m: Int = hGraph.foldLeft(0)(_ + _.getNumEdges)
      (n, m)
    }
    else
      (0, 0)
  }

  def exists(vId: Int): Boolean = {
    vertices.contains(vId)
  }

  def resetGetters {
    firstGet = true
  }

  def reset = synchronized {
    initialized = false
    firstGet = false
    vertices.clear()
    hGraph = null
  }
}
