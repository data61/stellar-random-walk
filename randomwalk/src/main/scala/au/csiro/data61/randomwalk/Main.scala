package au.csiro.data61.randomwalk

import au.csiro.data61.randomwalk.algorithm.{UniformRandomWalk, VCutRandomWalk}
import au.csiro.data61.randomwalk.common.CommandParser.TaskName
import au.csiro.data61.randomwalk.common.{CommandParser, Params, Property}
import com.typesafe.config.Config
import org.apache.log4j.LogManager
import org.apache.spark.mllib.feature.{Word2Vec, Word2VecModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.scalactic.{Every, Good, Or}
import spark.jobserver.SparkJobInvalid
import spark.jobserver.api._

object Main extends SparkJob {
  lazy val logger = LogManager.getLogger("myLogger")

  def main(args: Array[String]) {
    CommandParser.parse(args) match {
      case Some(params) =>
        val conf = new SparkConf().setAppName("stellar-random-walk")
        val context: SparkContext = new SparkContext(conf)
        runJob(context, null, params)

      case None => sys.exit(1)
    }
  }

  /**
    * Saves the word2vec model and features in separate files.
    *
    * @param model
    * @param context
    * @param config
    */
  private def saveModelAndFeatures(model: Word2VecModel, context: SparkContext, config: Params)
  : Unit = {
    model.save(context, s"${config.output}/${Property.modelSuffix}")
    val numPartitions = getNumOutputPartition(config)
    context.parallelize(model.getVectors.toList, config.rddPartitions).map { case (nodeId,
    vector) =>
      s"$nodeId\t${vector.mkString("\t")}"
    }.repartition(numPartitions).saveAsTextFile(s"${config.output}/${Property.vectorSuffix}")
  }

  /**
    * Runs random-walk configured based on the input parameters.
    *
    * @param context
    * @param param input parameters.
    * @return
    */
  def doRandomWalk(context: SparkContext, param: Params): RDD[Array[Int]] = {
    val rw = param.partitioned match {
      case true => VCutRandomWalk(context, param)
      case false => UniformRandomWalk(context, param)
    }
    val paths = rw.execute()
    val numPartitions = getNumOutputPartition(param)
    rw.save(paths, numPartitions, param.output)
    paths
  }

  private def getNumOutputPartition(param: Params): Int = {
    param.singleOutput match {
      case true => 1
      case false => param.rddPartitions
    }
  }

  /**
    * Converts sequences of vertex ids to the format accepted by Word2vec.
    *
    * @param paths
    * @return
    */
  def convertPathsToIterables(paths: RDD[Array[Int]]) = {
    paths.map { p =>
      p.map(_.toString).toList
    }
  }

  /**
    * Setups an instance of MLlib's Word2vec object.
    *
    * @param param
    * @return
    */
  private def configureWord2Vec(param: Params): Word2Vec = {
    val word2vec = new Word2Vec()
    word2vec.setLearningRate(param.w2vLr)
      .setNumIterations(param.w2vIter)
      .setNumPartitions(param.w2vPartitions)
      .setMinCount(0)
      .setVectorSize(param.w2vDim)
      .setWindowSize(param.w2vWindow)
  }

  override type JobData = Params
  override type JobOutput = String

  /**
    *
    * @param context
    * @param runtime
    * @param params input parameters.
    * @return
    */
  override def runJob(context: SparkContext, runtime: JobEnvironment, params: JobData): JobOutput
  = {

    params.cmd match {
      case TaskName.node2vec =>
        val paths = doRandomWalk(context, params)
        val word2Vec = configureWord2Vec(params)
        val model = word2Vec.fit(convertPathsToIterables(paths))
        saveModelAndFeatures(model, context, params)
      case TaskName.randomwalk => doRandomWalk(context, params)
      case TaskName.embedding =>
        val paths = context.textFile(params.input).repartition(params.rddPartitions).
          map(_.split("\\s+").toSeq)
        val word2Vec = configureWord2Vec(params)
        val model = word2Vec.fit(paths)
        saveModelAndFeatures(model, context, params)
    }
    params.output
  }

  /**
    * Validates the given config (required as a Job-server app).
    *
    * @param sc
    * @param runtime
    * @param config input parameters.
    * @return
    */
  override def validate(sc: SparkContext, runtime: JobEnvironment, config: Config): JobData Or
    Every[SparkJobInvalid] = {
    val args = config.getString("rw.input").split("\\s+")
    CommandParser.parse(args) match {
      case Some(params) => Good(params)
    }
  }
}
