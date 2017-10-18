package com.navercorp

import java.io.Serializable

import com.navercorp.Main.Params
import com.navercorp.graph.GraphOps
import org.apache.spark.{SparkConf, SparkContext}
import scopt.OptionParser
import com.navercorp.lib.AbstractParams
import org.apache.log4j.LogManager
import org.apache.spark.rdd.RDD
import com.navercorp.graph.{EdgeAttr, NodeAttr}
import com.twitter.chill.Kryo
import org.apache.spark.graphx.GraphXUtils
import org.apache.spark.serializer.KryoRegistrator

object Main {
  //  lazy val logger: Logger = LoggerFactory.getLogger(getClass.getName)
  lazy val logger = LogManager.getLogger("myLogger")
  val useKyroSerializer: Boolean = true

  object Command extends Enumeration {
    type Command = Value
    val node2vec, randomwalk, embedding = Value
  }

  import Command._

  case class Params(iter: Int = 10,
                    lr: Double = 0.025,
                    numPartition: Int = 10,
                    dim: Int = 128,
                    window: Int = 10,
                    walkLength: Int = 80,
                    numWalks: Int = 10,
                    p: Double = 1.0,
                    q: Double = 1.0,
                    weighted: Boolean = true,
                    directed: Boolean = false,
                    degree: Int = 30, // Max number of neighbors to consider for random walk.
                    indexed: Boolean = true,
                    nodePath: String = null,
                    input: String = null,
                    output: String = null,
                    cmd: Command = Command.node2vec) extends AbstractParams[Params] with
    Serializable

  val defaultParams = Params()

  val parser = new OptionParser[Params]("Node2Vec_Spark") {
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
    opt[Boolean]("weighted")
      .text(s"weighted: ${defaultParams.weighted}")
      .action((x, c) => c.copy(weighted = x))
    opt[Boolean]("directed")
      .text(s"directed: ${defaultParams.directed}")
      .action((x, c) => c.copy(directed = x))
    opt[Int]("degree")
      .text(s"degree: ${defaultParams.degree}")
      .action((x, c) => c.copy(degree = x))
    opt[Boolean]("indexed")
      .text(s"Whether nodes are indexed or not: ${defaultParams.indexed}")
      .action((x, c) => c.copy(indexed = x))
    opt[String]("nodePath")
      .text("Input node2index file path: empty")
      .action((x, c) => c.copy(nodePath = x))
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
    note(
      """
        |For example, the following command runs this app on a synthetic dataset:
        |
        | bin/spark-submit --class com.nhn.sunny.vegapunk.ml.model.Node2vec \
      """.stripMargin +
        s"|   --lr ${defaultParams.lr}" +
        s"|   --iter ${defaultParams.iter}" +
        s"|   --numPartition ${defaultParams.numPartition}" +
        s"|   --dim ${defaultParams.dim}" +
        s"|   --window ${defaultParams.window}" +
        s"|   --input <path>" +
        s"|   --node <nodeFilePath>" +
        s"|   --output <path>"
    )
  }

  def main(args: Array[String]) {
    val node2vec = new Node2vec()
    val word2vec = new Word2vec()
    parser.parse(args, defaultParams).map { param =>
      val conf = new SparkConf().setAppName("Node2Vec")
      if (useKyroSerializer) {
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        //        conf.set("spark.kryo.registrator", "com.navercorp.MyRegistrator")
        conf.set("spark.kryo.registrationRequired", "true")
        //      conf.registerKryoClasses(Array(classOf[Node2vec], classOf[Word2vec],
        // classOf[NodeAttr],
        //        classOf[EdgeAttr]))
        conf.registerKryoClasses(Array(classOf[Node2vec], classOf[Word2vec], classOf[NodeAttr],
          classOf[EdgeAttr], classOf[Params], classOf[Array[(Long, Double)]],
          classOf[Array[Int]], classOf[Array[Long]]))
        GraphXUtils.registerKryoClasses(conf)
      }
      val context: SparkContext = new SparkContext(conf)

      GraphOps.setup(context, param)
      node2vec.setup(context, param)
      word2vec.setup(context, param)

      param.cmd match {
        case Command.node2vec =>
          logger.warn("Loading graph...")
          val graph = node2vec.loadGraph()
          logger.warn("Starting random walk...")
          val randomPaths: RDD[String] = node2vec.randomWalk(graph)
          node2vec.save(randomPaths)
          word2vec.readFromRdd(randomPaths).fit().save()
        case Command.randomwalk =>
          logger.warn("Loading graph...")
          val graph = node2vec.loadGraph()
          logger.warn("Starting random walk...")
          val randomPaths: RDD[String] = node2vec.randomWalk(graph)
          logger.warn("Completed random walk...")
          node2vec.save(randomPaths)

        case Command.embedding => {
          val randomPaths = word2vec.read(param.input)
          word2vec.fit().save()
        }
      }
    } getOrElse {
      sys.exit(1)
    }
  }
}

//class MyRegistrator extends KryoRegistrator {
//  override def registerClasses(kryo: Kryo) {
//    kryo.register(classOf[Node2vec])
//    kryo.register(classOf[Word2vec])
//    kryo.register(classOf[NodeAttr])
//    kryo.register(classOf[EdgeAttr])
//    kryo.register(classOf[Params])
//  }
//}
