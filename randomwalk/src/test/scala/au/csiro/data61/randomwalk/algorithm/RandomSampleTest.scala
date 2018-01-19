package au.csiro.data61.randomwalk.algorithm

import org.apache.spark.graphx.Edge
import org.scalatest.FunSuite

class RandomSampleTest extends FunSuite {

  // TODO assert can move to a function for DRY purpose.
  test("Test random sample function") {
    var rValue = 0.1f
    var sample = RandomSample.sample(nextFloat = () => rValue)_
    val e1 = (1, 1.0f)
    val e2 = (2, 1.0f)
    val e3 = (3, 1.0f)
    val edges = Array(e1, e2, e3)
    assert(sample(edges) == e1)
    rValue = 0.4f
    sample = RandomSample.sample(nextFloat = () => rValue)_
    assert(sample(edges) == e2)
    rValue = 0.7f
    sample = RandomSample.sample(nextFloat = () => rValue)_
    assert(sample(edges) == e3)
  }

  test("Test second order random selection") {
    val w1 = 1.0f
    val e12 = (2, w1)
    val e21 = (1, w1)
    val e23 = (3, w1)
    val e24 = (4, w1)
    val e14 = (4, w1)
    val e15 = (5, w1)

    val prevId = 1
    var prevNeighbors = Array(e12, e14, e15)
    val currNeighbors = Array(e21, e23, e24)
    var p = 1.0f
    var q = 1.0f

    var biasedWeights = RandomSample.computeSecondOrderWeights(p, q, prevId, prevNeighbors,
      currNeighbors)
    assert(biasedWeights sameElements currNeighbors)

    var rValue = 0.1f
    var soSample = RandomSample.secondOrderSample(nextFloat = () => rValue)_
    assert(soSample(p, q, prevId, prevNeighbors, currNeighbors) == e21)

    rValue = 0.4f
    soSample = RandomSample.secondOrderSample(nextFloat = () => rValue)_
    assert(soSample(p, q, prevId, prevNeighbors, currNeighbors) == e23)

    rValue = 0.7f
    soSample = RandomSample.secondOrderSample(nextFloat = () => rValue)_
    assert(soSample(p, q, prevId, prevNeighbors, currNeighbors) == e24)

    p = 2.0f
    q = 2.0f
    prevNeighbors = Array(e12, e15)

    biasedWeights = RandomSample.computeSecondOrderWeights(p, q, prevId, prevNeighbors,
      currNeighbors)
    var e1 = (currNeighbors(0)._1, w1 / p)
    var e2 = (currNeighbors(1)._1, w1 / q)
    var e3 = (currNeighbors(2)._1, w1 / q)
    assert(biasedWeights sameElements Array(e1, e2, e3))

    prevNeighbors = Array(e12, e14, e15)
    rValue = 0.24f
    soSample = RandomSample.secondOrderSample(nextFloat = () => rValue)_
    biasedWeights = RandomSample.computeSecondOrderWeights(p, q, prevId, prevNeighbors,
      currNeighbors)
    e1 = (currNeighbors(0)._1, w1 / p)
    e2 = (currNeighbors(1)._1, w1 / q)
    e3 = (currNeighbors(2)._1, w1)
    assert(biasedWeights sameElements Array(e1, e2, e3))
    assert(soSample(p, q, prevId, prevNeighbors, currNeighbors) == e1)

    rValue = 0.26f
    soSample = RandomSample.secondOrderSample(nextFloat = () => rValue)_
    assert(soSample(p, q, prevId, prevNeighbors, currNeighbors) == e2)

    rValue = 0.51f
    soSample = RandomSample.secondOrderSample(nextFloat = () => rValue)_
    assert(soSample(p, q, prevId, prevNeighbors, currNeighbors) == e3)

    rValue = 0.99f
    soSample = RandomSample.secondOrderSample(nextFloat = () => rValue)_
    assert(soSample(p, q, prevId, prevNeighbors, currNeighbors) == e3)

    // check if orginal weights are unchanged
    assert(currNeighbors(0)._2 == w1)
    assert(currNeighbors(1)._2 == w1)
    assert(currNeighbors(2)._2 == w1)
  }

  def checkResult(result: Option[Edge[Float]], expected: Edge[Float]): Boolean = {
    result match {
      case Some(e) => e.equals(expected)
      case None => false
    }
  }
}
