package au.csiro.data61.randomwalk

import au.csiro.data61.randomwalk.common.{AbstractParams, Property}
import au.csiro.data61.randomwalk.random.UniformRandomWalk
import au.csiro.data61.randomwalk.vertexcut.VCutRandomWalk
import org.apache.log4j.LogManager
import org.apache.spark.mllib.feature.{Word2Vec, Word2VecModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scopt.OptionParser

object Main {
  lazy val logger = LogManager.getLogger("myLogger")

  object Command extends Enumeration {
    type Command = Value
    val node2vec, randomwalk, embedding = Value
  }

  import Command._

  case class Params(w2vIter: Int = 10,
                    w2vLr: Double = 0.025,
                    w2vPartitions: Int = 10,
                    w2vDim: Int = 128,
                    w2vWindow: Int = 10,
                    walkLength: Int = 80,
                    numWalks: Int = 10,
                    p: Double = 1.0,
                    q: Double = 1.0,
                    weighted: Boolean = true,
                    directed: Boolean = false,
                    input: String = null,
                    output: String = null,
                    useKyroSerializer: Boolean = false,
                    rddPartitions: Int = 200,
                    partitioned: Boolean = false,
                    cmd: Command = Command.node2vec) extends AbstractParams[Params]

  val defaultParams = Params()

  val parser = new OptionParser[Params]("2nd Order Random Walk + Word2Vec") {
    head("Main")
    opt[Int]("walkLength")
      .text(s"walkLength: ${defaultParams.walkLength}")
      .action((x, c) => c.copy(walkLength = x))
    opt[Int]("numWalks")
      .text(s"numWalks: ${defaultParams.numWalks}")
      .action((x, c) => c.copy(numWalks = x))
    opt[Double]("p")
      .text(s"return parameter p: ${defaultParams.p}")
      .action((x, c) => c.copy(p = x))
    opt[Double]("q")
      .text(s"in-out parameter q: ${defaultParams.q}")
      .action((x, c) => c.copy(q = x))
    opt[Int]("rddPartitions")
      .text(s"Number of RDD partitions in running Random Walk and Word2vec: ${
        defaultParams
          .rddPartitions
      }")
      .action((x, c) => c.copy(rddPartitions = x))
    opt[Boolean]("weighted")
      .text(s"weighted: ${defaultParams.weighted}")
      .action((x, c) => c.copy(weighted = x))
    opt[Boolean]("directed")
      .text(s"directed: ${defaultParams.directed}")
      .action((x, c) => c.copy(directed = x))
    opt[Int]("w2vPartitions")
      .text(s"Number of partitions in word2vec: ${defaultParams.w2vPartitions}")
      .action((x, c) => c.copy(w2vPartitions = x))
    opt[String]("input")
      .required()
      .text("Input edge file path: empty")
      .action((x, c) => c.copy(input = x))
    opt[String]("output")
      .required()
      .text("Output path: empty")
      .action((x, c) => c.copy(output = x))
    opt[String]("cmd")
      .required()
      .text(s"command: ${defaultParams.cmd.toString}")
      .action((x, c) => c.copy(cmd = Command.withName(x)))
    opt[Boolean]("kryo")
      .text(s"Whether to use kryo serializer or not: ${defaultParams.useKyroSerializer}")
      .action((x, c) => c.copy(useKyroSerializer = x))
    opt[Boolean]("partitioned")
      .text(s"Whether the graph is partitioned: ${defaultParams.partitioned}")
      .action((x, c) => c.copy(partitioned = x))
    opt[Double]("lr")
      .text(s"Learning rate in word2vec: ${defaultParams.w2vLr}")
      .action((x, c) => c.copy(w2vLr = x))
    opt[Int]("iter")
      .text(s"Number of iterations in word2vec: ${defaultParams.w2vIter}")
      .action((x, c) => c.copy(w2vIter = x))
    opt[Int]("dim")
      .text(s"Number of dimensions in word2vec: ${defaultParams.w2vDim}")
      .action((x, c) => c.copy(w2vDim = x))
    opt[Int]("window")
      .text(s"Window size in word2vec: ${defaultParams.w2vWindow}")
      .action((x, c) => c.copy(w2vWindow = x))
    note(
      """
        |For example, the following command runs this app on a synthetic dataset:
        |
        | bin/spark-submit --class au.csiro.data61.randomwalk.Main \
      """.stripMargin +
        s"|   --lr ${defaultParams.w2vLr}" +
        s"|   --iter ${defaultParams.w2vIter}" +
        s"|   --w2vPartition ${defaultParams.w2vPartitions}" +
        s"|   --dim ${defaultParams.w2vDim}" +
        s"|   --window ${defaultParams.w2vWindow}" +
        s"|   --input <path>" +
        s"|   --output <path>"
    )
  }

  def main(args: Array[String]) {
    parser.parse(args, defaultParams).map { param =>
      val conf = new SparkConf().setAppName("Node2Vec")
      if (param.useKyroSerializer) {
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        conf.set("spark.kryo.registrationRequired", "true")
        //TODO: Register the newly added classes.
        //        conf.registerKryoClasses(Array(
        //          classOf[NodeAttr],
        //          classOf[EdgeAttr],
        //          classOf[Array[NodeAttr]],
        //          classOf[Array[EdgeAttr]]))
        //        GraphXUtils.registerKryoClasses(conf)
      }
      val context: SparkContext = new SparkContext(conf)

      param.cmd match {
        case Command.node2vec =>
          val paths = doRandomWalk(context, param)
          val word2Vec = configureWord2Vec(param)
          val model = word2Vec.fit(convertPathsToIterables(paths))
          saveModelAndFeatures(model, context, param)
        case Command.randomwalk => doRandomWalk(context, param)

        case Command.embedding => {
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
    context.parallelize(model.getVectors.toList, config.w2vPartitions).map { case (nodeId, vector) =>
      s"$nodeId\t${vector.mkString(",")}"
    }.saveAsTextFile(s"${config.output}.${Property.vectorSuffix}")
  }

  def doRandomWalk(context: SparkContext, param: Params): RDD[List[Int]] = {
    val rw = param.partitioned match {
      case true => VCutRandomWalk(context, param)
      case false => UniformRandomWalk(context, param)
    }
    val paths = rw.execute()
    rw.save(paths, param.rddPartitions, param.output)
    paths
  }

  def convertPathsToIterables(paths: RDD[List[Int]]) = {
    paths.map { p =>
      p.map(_.toString)
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
