package au.csiro.data61.randomwalk.efficient

import org.apache.spark.graphx.Edge
import org.scalatest.FunSuite

class RandomSampleTest extends FunSuite {

  // TODO assert can move to a function for DRY purpose.
  test("Test random sample function") {
    var rValue = 0.1
    val pId = 0
    var random = RandomSample(nextDouble = () => rValue)
    assert(random.nextDouble() == rValue)
    val e1 = (1L, pId, 1.0)
    val e2 = (2L, pId,1.0)
    val e3 = (3L, pId,1.0)
    val edges = Array(e1, e2, e3)
    assert(random.sample(edges) == e1)
    rValue = 0.4
    random = RandomSample(nextDouble = () => rValue)
    assert(random.sample(edges) == e2)
    rValue = 0.7
    random = RandomSample(nextDouble = () => rValue)
    assert(random.sample(edges) == e3)
  }

  test("Test second order random selection") {
    val pId = 0
    val w1 = 1.0
    val e12 = (2L, pId, w1)
    val e21 = (1L, pId, w1)
    val e23 = (3L, pId, w1)
    val e24 = (4L, pId,  w1)
    val e14 = (4L, pId, w1)
    val e15 = (5L, pId, w1)

    val prevId = 1
    var prevNeighbors = Array(e12, e14, e15)
    val currNeighbors = Array(e21, e23, e24)
    var p = 1.0
    var q = 1.0

    var random = RandomSample()
    var biasedWeights = random.computeSecondOrderWeights(p, q, prevId, prevNeighbors,
      currNeighbors)
    assert(biasedWeights sameElements currNeighbors)

    var rValue = 0.1
    random = RandomSample(nextDouble = () => rValue)
    assert(random.secondOrderSample(p, q, prevId, prevNeighbors, currNeighbors) == e21)

    rValue = 0.4
    random = RandomSample(nextDouble = () => rValue)
    assert(random.secondOrderSample(p, q, prevId, prevNeighbors, currNeighbors) == e23)

    rValue = 0.7
    random = RandomSample(nextDouble = () => rValue)
    assert(random.secondOrderSample(p, q, prevId, prevNeighbors, currNeighbors) == e24)

    p = 2.0
    q = 2.0
    prevNeighbors = Array(e12, e15)

    biasedWeights = random.computeSecondOrderWeights(p, q, prevId, prevNeighbors, currNeighbors)
    var e1 = (currNeighbors(0)._1, w1 / p)
    var e2 = (currNeighbors(1)._1, w1 / q)
    var e3 = (currNeighbors(2)._1, w1 / q)
    assert(biasedWeights sameElements Array(e1, e2, e3))

    prevNeighbors = Array(e12, e14, e15)
    rValue = 0.24
    random = RandomSample(nextDouble = () => rValue)
    biasedWeights = random.computeSecondOrderWeights(p, q, prevId, prevNeighbors, currNeighbors)
    e1 = (currNeighbors(0)._1, w1 / p)
    e2 = (currNeighbors(1)._1, w1 / q)
    e3 = (currNeighbors(2)._1, w1)
    assert(biasedWeights sameElements Array(e1, e2, e3))
    assert(random.secondOrderSample(p, q, prevId, prevNeighbors, currNeighbors) == e1)

    rValue = 0.26
    random = RandomSample(nextDouble = () => rValue)
    assert(random.secondOrderSample(p, q, prevId, prevNeighbors, currNeighbors) == e2)

    rValue = 0.51
    random = RandomSample(nextDouble = () => rValue)
    assert(random.secondOrderSample(p, q, prevId, prevNeighbors, currNeighbors) == e3)

    rValue = 0.99
    random = RandomSample(nextDouble = () => rValue)
    assert(random.secondOrderSample(p, q, prevId, prevNeighbors, currNeighbors) == e3)

    // check if orginal weights are unchanged
    assert(currNeighbors(0)._2 == w1)
    assert(currNeighbors(1)._2 == w1)
    assert(currNeighbors(2)._2 == w1)
  }

  def checkResult(result: Option[Edge[Double]], expected: Edge[Double]): Boolean = {
    result match {
      case Some(e) => e.equals(expected)
      case None => false
    }
  }
}
