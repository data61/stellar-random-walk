package au.csiro.data61

import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap}

import org.apache.spark.graphx.{Edge, VertexId}

/**
  *
  */
case class GraphMap() extends Serializable {

  private var srcVertexMap: ConcurrentMap[Long, Int] = _
  private var lengths: Array[Int] = _
  private var offsets: Array[Int] = _
  private var edges: Array[(Long, Double)] = _

  /**
    *
    *
    * @param verticesToNeighbors
    * @param numEdges
    */
  def setUp(verticesToNeighbors: Array[(Long, Array[Edge[Double]])], numEdges: Int) {
    // Not having a vId in the map can mean either: (i) the vertex has no outbound edge
    val numVertices = verticesToNeighbors.length
    srcVertexMap = new ConcurrentHashMap(numVertices)
    offsets = new Array(numVertices)
    lengths = new Array(numVertices)
    edges = new Array(numEdges)

    var eCounter: Int = 0
    for (index <- 0 until numVertices) {
      val (vId: Long, neighbors: Array[Edge[Double]]) = verticesToNeighbors(index)
      srcVertexMap.put(vId, index)
      offsets(index) = eCounter
      lengths(index) = neighbors.length
      for (e <- 0 until neighbors.length) {
        edges(eCounter) = (neighbors(e).dstId, neighbors(e).attr)
        eCounter += 1
      }
    }
  }

  def numVertices: Int = {
    offsets.length
  }

  def numEdges: Int = {
    edges.length
  }

  /**
    *
    * @param vid
    * @return (offset, length)
    */
  //  def getNeighborsIndices(vid: Long): (Int, Int) = {
  //    val index = srcVertexMap.get(vid)
  //    val offset = offsets(index)
  //    val length = lengths(index)
  //    (offset, length)
  //  }

  def getNeighbors(vid: Long): Array[(Long, Double)] = {
    val index = srcVertexMap.get(vid)
    val offset = offsets(index)
    val length = lengths(index)
    edges.slice(offset, offset + length)
  }
}
