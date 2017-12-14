package au.csiro.data61.randomwalk

import au.csiro.data61.randomwalk.algorithm.VCutRandomWalk
import au.csiro.data61.randomwalk.common.CommandParser.TaskName
import au.csiro.data61.randomwalk.common.{CommandParser, Params, Property}
import au.csiro.data61.randomwalk.algorithm.UniformRandomWalk
import org.apache.log4j.LogManager
import org.apache.spark.mllib.feature.{Word2Vec, Word2VecModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Main {
  lazy val logger = LogManager.getLogger("myLogger")

  def main(args: Array[String]) {
    CommandParser.parse(args).map { param =>
      val conf = new SparkConf().setAppName("Node2Vec")
      if (param.useKyroSerializer) {
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        conf.set("spark.kryo.registrationRequired", "true")
        //TODO: Register the newly added classes.
        //        conf.registerKryoClasses(Array(...))
      }
      val context: SparkContext = new SparkContext(conf)
      param.cmd match {
        case TaskName.node2vec =>
          val paths = doRandomWalk(context, param)
          val word2Vec = configureWord2Vec(param)
          val model = word2Vec.fit(convertPathsToIterables(paths))
          saveModelAndFeatures(model, context, param)
        case TaskName.randomwalk => doRandomWalk(context, param)

        case TaskName.embedding => {
          val paths = context.textFile(param.input).repartition(param.w2vPartitions).
            map(_.split("\\s+").toSeq)
          val word2Vec = configureWord2Vec(param)
          val model = word2Vec.fit(paths)
          saveModelAndFeatures(model, context, param)
        }
      }
    } getOrElse {
      sys.exit(1)
    }
  }

  private def saveModelAndFeatures(model: Word2VecModel, context: SparkContext, config: Params)
  : Unit = {
    model.save(context, s"${config.output}.${Property.modelSuffix}")
    context.parallelize(model.getVectors.toList, config.w2vPartitions).map { case (nodeId,
    vector) =>
      s"$nodeId\t${vector.mkString(",")}"
    }.saveAsTextFile(s"${config.output}.${Property.vectorSuffix}")
  }

  def doRandomWalk(context: SparkContext, param: Params): RDD[Array[Int]] = {
    val rw = param.partitioned match {
      case true => VCutRandomWalk(context, param)
      case false => UniformRandomWalk(context, param)
    }
    val paths = rw.execute()
    rw.save(paths, param.rddPartitions, param.output)
    paths
  }

  def convertPathsToIterables(paths: RDD[Array[Int]]) = {
    paths.map { p =>
      p.map(_.toString).toList
    }
  }

  private def configureWord2Vec(param: Params): Word2Vec = {
    val word2vec = new Word2Vec()
    word2vec.setLearningRate(param.w2vLr)
      .setNumIterations(param.w2vIter)
      .setNumPartitions(param.w2vPartitions)
      .setMinCount(0)
      .setVectorSize(param.w2vDim)
      .setWindowSize(param.w2vWindow)
  }
}
