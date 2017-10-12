import scala.util.Try

def sum(f: String => String):Int = {
  1
}

sum(x => x+x)
val x = "2"

Try(x.toDouble).getOrElse(1)


var arr = Array((1,2),(2,3), (0,1),(1,2),(2,3), (0,1))
arr.zipWithIndex
var arr2 = Array((4,5))
arr ++ arr2

var a = "Hooman"

val b = (1,2)

b._1

var c = arr.groupBy(_._1).map{case (group, traversable) =>
  traversable.head
}



arr.sortWith{case (left, right) => left._1 > right._1}.slice(0,5)



