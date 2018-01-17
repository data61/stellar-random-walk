package au.csiro.data61.randomwalk.common

object Property extends Enumeration {
  private val suffix = (System.currentTimeMillis()/1000).toString
  val countsSuffix = Value(s"counts-${suffix}")
  val pathSuffix = Value(s"path-${suffix}")
}
