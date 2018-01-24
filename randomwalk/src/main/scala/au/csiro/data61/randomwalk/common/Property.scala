package au.csiro.data61.randomwalk.common

object Property extends Enumeration {
  private val suffix = (System.currentTimeMillis()/1000).toString
  val countsSuffix = Value(s"counts-${suffix}")
  val pathSuffix = Value(s"path-${suffix}")
  val probSuffix = Value(s"probs-${suffix}")
  val degreeSuffix = Value(s"degrees-${suffix}")
}
