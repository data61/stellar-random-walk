package au.csiro.data61.randomwalk.algorithm

import org.apache.spark.graphx.Edge
import org.scalatest.FunSuite

class RandomSampleTest extends FunSuite {

  // TODO assert can move to a function for DRY purpose.
  test("Test random sample function") {
    var rValue = 0.1f
    var random = RandomSample(nextFloat = () => rValue)
    assert(random.nextFloat() == rValue)
    val e1 = (1, 1.0f)
    val e2 = (2, 1.0f)
    val e3 = (3, 1.0f)
    val edges = Array(e1, e2, e3)
    assert(random.sample(edges) == e1)
    rValue = 0.4f
    random = RandomSample(nextFloat = () => rValue)
    assert(random.sample(edges) == e2)
    rValue = 0.7f
    random = RandomSample(nextFloat = () => rValue)
    assert(random.sample(edges) == e3)
  }

  def checkResult(result: Option[Edge[Float]], expected: Edge[Float]): Boolean = {
    result match {
      case Some(e) => e.equals(expected)
      case None => false
    }
  }
}
