package au.csiro.data61.randomwalk.vertexcut

import au.csiro.data61.randomwalk.GraphMap

import scala.collection.mutable
import scala.collection.mutable.HashMap


/**
  *
  */

object PartitionedGraphMap extends GraphMap[(Int, Int, Float)] {

  private lazy val vertexPartitionMap: mutable.Map[Int, Int] = new HashMap[Int, Int]()

  def addVertex(vId: Int, neighbors: Array[(Int, Int, Float)]): Unit = synchronized {
    srcVertexMap.get(vId) match {
      case None => {
        if (!neighbors.isEmpty) {
          srcVertexMap.put(vId, indexCounter)
          offsets.insert(indexCounter, offsetCounter)
          lengths.insert(indexCounter, neighbors.length)
          for ((dst, pId, weight) <- neighbors) {
            edges.insert(offsetCounter, (dst, weight))
            offsetCounter += 1
            vertexPartitionMap.put(dst, pId)
          }

          indexCounter += 1
        } else {
          this.addVertex(vId)
        }
      }
      case Some(value) => value
    }
  }

  def getPartition(vId: Int): Option[Int] = {
    vertexPartitionMap.get(vId)
  }

  /**
    * The reset is mainly for the unit test purpose. It does not reset the size of data
    * structures that are initialy set by calling setUp function.
    */
  override def reset {
    super.reset
    vertexPartitionMap.clear
  }
}
