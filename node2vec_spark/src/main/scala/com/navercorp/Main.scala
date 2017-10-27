package com.navercorp

import java.io.Serializable

import com.navercorp.graph.{EdgeAttr, GraphOps, NodeAttr}
import com.navercorp.lib.AbstractParams
import org.apache.log4j.LogManager
import org.apache.spark.graphx.GraphXUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scopt.OptionParser

object Main {
  lazy val logger = LogManager.getLogger("myLogger")

  object Command extends Enumeration {
    type Command = Value
    val node2vec, o_randomwalk, s_randomwalk, embedding = Value
  }

  import Command._

  case class Params(iter: Int = 10,
                    lr: Double = 0.025,
                    w2vPartitions: Int = 10,
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
                    useKyroSerializer: Boolean = false,
                    rddPartitions: Int = 200,
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
    opt[Int]("degree")
      .text(s"degree: ${defaultParams.degree}")
      .action((x, c) => c.copy(degree = x))
    opt[Boolean]("indexed")
      .text(s"Whether nodes are indexed or not: ${defaultParams.indexed}")
      .action((x, c) => c.copy(indexed = x))
    opt[String]("nodePath")
      .text("Input node2index file path: empty")
      .action((x, c) => c.copy(nodePath = x))
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
    note(
      """
        |For example, the following command runs this app on a synthetic dataset:
        |
        | bin/spark-submit --class com.nhn.sunny.vegapunk.ml.model.Node2vec \
      """.stripMargin +
        s"|   --lr ${defaultParams.lr}" +
        s"|   --iter ${defaultParams.iter}" +
        s"|   --w2vPartition ${defaultParams.w2vPartitions}" +
        s"|   --dim ${defaultParams.dim}" +
        s"|   --window ${defaultParams.window}" +
        s"|   --input <path>" +
        s"|   --node <nodeFilePath>" +
        s"|   --output <path>"
    )
  }

  def main(args: Array[String]) {
    parser.parse(args, defaultParams).map { param =>
      val conf = new SparkConf().setAppName("Node2Vec")
      if (param.useKyroSerializer) {
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        conf.set("spark.kryo.registrationRequired", "true")
        conf.registerKryoClasses(Array(
          classOf[NodeAttr],
          classOf[EdgeAttr],
          classOf[Array[NodeAttr]],
          classOf[Array[EdgeAttr]]))
        GraphXUtils.registerKryoClasses(conf)
      }
      val context: SparkContext = new SparkContext(conf)

      param.cmd match {
        case Command.node2vec =>
        //          val graph = Node2vec.loadGraph()
        //          val randomPaths: RDD[String] = Node2vec.randomWalk(graph)
        //          Node2vec.save(randomPaths)
        //          Word2vec.readFromRdd(randomPaths).fit().save()
        case Command.s_randomwalk =>
          val rw = RandomWalk(context, param)
          val graph = rw.loadGraph()
          val paths = rw.randomWalk(graph)
          rw.save(paths)
        case Command.o_randomwalk =>
          GraphOps.setup(context, param)
          Node2vec.setup(context, param)
          Word2vec.setup(context, param)
          val graph = Node2vec.loadGraph()
          val randomPaths: RDD[String] = Node2vec.randomWalk(graph)
          logger.warn("Completed random walk...")
          Node2vec.save(randomPaths)
        case Command.embedding => {
          //          val randomPaths = Word2vec.read(param.input)
          //          Word2vec.fit().save()
        }
      }
    } getOrElse {
      sys.exit(1)
    }
  }
}
