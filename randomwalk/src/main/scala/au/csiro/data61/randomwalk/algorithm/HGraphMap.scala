package au.csiro.data61.randomwalk.algorithm

object HGraphMap {
  private var firstGet: Boolean = true

  var hGraph: Array[GraphMap] = _

  def initGraphMap(numVertexTypes: Int): HGraphMap.type = {
    hGraph = new Array[GraphMap](numVertexTypes).map(_ => GraphMap())
    this
  }

  def getGraphMap(vType: Short): GraphMap = {
    hGraph(vType)
  }

  def getNeighbors(vId: Int, vType: Short): Array[(Int, Float)] = {
    hGraph(vType).getNeighbors(vId)
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

  def resetGetters {
    firstGet = true
  }
}
