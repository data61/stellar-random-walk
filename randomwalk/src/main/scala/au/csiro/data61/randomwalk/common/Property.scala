package au.csiro.data61.randomwalk.common

object Property extends Enumeration {
//  private val suffix = (System.currentTimeMillis()/1000).toString
  val modelSuffix = Value(s"bin")
  val pathSuffix = Value(s"path")
  val vectorSuffix = Value(s"vec")
}
