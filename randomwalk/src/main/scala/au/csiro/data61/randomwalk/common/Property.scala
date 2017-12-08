package au.csiro.data61.randomwalk.common

object Property extends Enumeration {
  private val suffix = (System.currentTimeMillis()/1000).toString
  val modelSuffix = Value(s"bin-${suffix}")
  val pathSuffix = Value(s"path-${suffix}")
  val vectorSuffix = Value(s"vec-${suffix}")
}
