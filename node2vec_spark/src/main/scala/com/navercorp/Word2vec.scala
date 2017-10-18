package com.navercorp

import com.navercorp.Main.Params
import org.apache.spark.SparkContext
import org.apache.spark.mllib.feature.{Word2Vec, Word2VecModel}
import org.apache.spark.rdd.RDD
import com.navercorp.common.Property

/**
  * Uses Word2vec implemented in Spark ML library.
  */
object Word2vec extends Serializable {
  var context: SparkContext = _
  var config: Params = _
  var model: Word2VecModel = _
  var word2vec = new Word2Vec()
  var examples: RDD[Iterable[String]] = _
  
  def setup(context: SparkContext, param: Main.Params): this.type = {
    this.context = context
    this.config = param
    /**
      * model = sg
      * update = hs
      * Note that number of partitions affect the accuracy.
      */
    word2vec.setLearningRate(param.lr)
            .setNumIterations(param.iter)
            .setNumPartitions(param.numPartition)
            .setMinCount(0)
            .setVectorSize(param.dim)

    val word2vecWindowField = word2vec.getClass.getDeclaredField("org$apache$spark$mllib$feature$Word2Vec$$window")
    word2vecWindowField.setAccessible(true)
    word2vecWindowField.setInt(word2vec, param.window)
    
    this
  }
  
  def read(path: String): this.type = {
    examples = context.textFile(path).repartition(200).map (_.split ("\\s").toIterable)
    this
  }
  def readFromRdd(randomPaths: RDD[String]): this.type = {
    examples = randomPaths.map (_.split ("\\s").toIterable)
    this
  }
  
  def fit(): this.type = {
    model = word2vec.fit(examples)
    this
  }
  
  def save(): this.type = {
    model.save(context, s"${config.output}.${Property.modelSuffix}")
    context.parallelize(model.getVectors.toList).map { case (nodeId, vector) =>
              s"$nodeId\t${vector.mkString(",")}"
            }.saveAsTextFile(s"${config.output}.${Property.vectorSuffix}")
  
    this
  }  
  
  def load(path: String): this.type = {
    model = Word2VecModel.load(context, path)
    this
  }
  
}

