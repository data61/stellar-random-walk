import com.navercorp.graph.NodeAttr
import org.apache.spark.{SparkConf, SparkContext}

val conf = new SparkConf().setAppName("Test").setMaster("local")
val sc = SparkContext.getOrCreate(conf)
val triplets = sc.parallelize(
  Array((1, 2L, 10.0), (2, 1L, 10.0), (1, 3L, 10.0), (2, 4L, 10.0))).flatMap { case (src, dst, weight) =>
  Array((src, Array((dst, weight))))
}.reduceByKey(_++_).map { case (srcId, neighbors: Array[(Long, Double)]) =>
  (srcId, NodeAttr(neighbors))
}
