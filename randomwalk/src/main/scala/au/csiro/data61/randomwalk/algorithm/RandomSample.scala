package au.csiro.data61.randomwalk.algorithm

import scala.util.Random

case class RandomSample(nextFloat: () => Float = Random.nextFloat) extends Serializable {


  /**
    *
    * @return
    */
  final def sample(edges: Array[(Int, Float)]): (Int, Float) = {

    val sum = edges.foldLeft(0.0) { case (w1, (_, w2)) => w1 + w2 }

    val p = nextFloat()
    var acc = 0.0
    for ((dstId, w) <- edges) {
      acc += w / sum
      if (acc >= p)
        return (dstId, w)
    }

    edges.head
  }
}
