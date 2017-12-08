package au.csiro.data61.randomwalk.random

import au.csiro.data61.randomwalk.GraphMap

/**
  *
  */

object RandomGraphMap extends GraphMap[(Int, Float)]{
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
}
