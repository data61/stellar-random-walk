package au.csiro.data61.randomwalk.naive

import org.apache.spark.graphx.Edge
import org.scalatest.FunSuite

class RandomSampleTest extends FunSuite {

  // TODO assert can move to a function for DRY purpose.
  test("Test random sample function") {
    var rValue = 0.1
    var random = RandomSample(nextDouble = () => rValue)
    assert(random.nextDouble() == rValue)
    val e1 = Edge(1, 1, 1.0)
    val e2 = Edge(2, 2, 1.0)
    val e3 = Edge(3, 3, 1.0)
    val edges = Array(e1, e2, e3)
    val weights = Array(1.0, 1.0, 1.0)
    assert(random.sampleIndex(weights) == 0)
    assert(checkResult(random.sample(edges), e1))
    rValue = 0.4
    random = RandomSample(nextDouble = () => rValue)
    assert(random.sampleIndex(weights) == 1)
    assert(checkResult(random.sample(edges), e2))
    rValue = 0.7
    random = RandomSample(nextDouble = () => rValue)
    assert(random.sampleIndex(weights) == 2)
    assert(checkResult(random.sample(edges), e3))
    // Test an empty edge array
    val noNeighbors = Array.empty[Edge[Double]]
    assert(random.sample(noNeighbors) == None)
  }

  test("Test second order random selection") {
    var w1 = 1.0
    val e12 = Edge(1, 2, w1)
    val e21 = Edge(2, 1, w1)
    val e23 = Edge(2, 3, w1)
    val e24 = Edge(2, 4, w1)
    val e14 = Edge(1, 4, w1)
    val e15 = Edge(1, 5, w1)

    val prevId = 1
    var prevNeighbors = Some(Array(e12, e14, e15))
    val currNeighbors = Array(e21, e23, e24)
    var p = 1.0
    var q = 1.0

    var random = RandomSample()
    var biasedWeights = random.computeSecondOrderWeights(p, q)(prevId, prevNeighbors, currNeighbors)
    assert(biasedWeights sameElements Array(w1, w1, w1))

    var rValue = 0.1
    random = RandomSample(nextDouble = () => rValue)
    assert(checkResult(random.secondOrderSample(p, q)(prevId, prevNeighbors, currNeighbors), e21))

    rValue = 0.4
    random = RandomSample(nextDouble = () => rValue)
    assert(checkResult(random.secondOrderSample(p, q)(prevId, prevNeighbors, currNeighbors), e23))

    rValue = 0.7
    random = RandomSample(nextDouble = () => rValue)
    assert(checkResult(random.secondOrderSample(p, q)(prevId, prevNeighbors, currNeighbors), e24))

    p = 2.0
    q = 2.0
    prevNeighbors = Some(Array(e12, e15))

    biasedWeights = random.computeSecondOrderWeights(p, q)(prevId, prevNeighbors, currNeighbors)
    assert(biasedWeights sameElements Array(w1 / p, w1 / q, w1 / q))

    prevNeighbors = Some(Array(e12, e14, e15))
    rValue = 0.24
    random = RandomSample(nextDouble = () => rValue)
    biasedWeights = random.computeSecondOrderWeights(p, q)(prevId, prevNeighbors, currNeighbors)
    assert(biasedWeights sameElements Array(w1 / p, w1 / q, w1))
    assert(checkResult(random.secondOrderSample(p, q)(prevId, prevNeighbors, currNeighbors), e21))

    rValue = 0.26
    random = RandomSample(nextDouble = () => rValue)
    assert(checkResult(random.secondOrderSample(p, q)(prevId, prevNeighbors, currNeighbors), e23))

    rValue = 0.51
    random = RandomSample(nextDouble = () => rValue)
    assert(checkResult(random.secondOrderSample(p, q)(prevId, prevNeighbors, currNeighbors), e24))

    rValue = 0.99
    random = RandomSample(nextDouble = () => rValue)
    assert(checkResult(random.secondOrderSample(p, q)(prevId, prevNeighbors, currNeighbors), e24))
  }

  def checkResult(result: Option[Edge[Double]], expected: Edge[Double]): Boolean = {
    result match {
      case Some(e) => e.equals(expected)
      case None => false
    }
  }
}
