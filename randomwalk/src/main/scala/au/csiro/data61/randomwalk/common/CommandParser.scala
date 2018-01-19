package au.csiro.data61.randomwalk.common

import scopt.OptionParser

object CommandParser {

  object TaskName extends Enumeration {
    type TaskName = Value
    val node2vec, randomwalk, embedding = Value
  }

  val WALK_LENGTH = "walkLength"
  val NUM_WALKS = "numWalks"
  val P = "p"
  val Q = "q"
  val RDD_PARTITIONS = "rddPartitions"
  val WEIGHTED = "weighted"
  val DIRECTED = "directed"
  val W2V_PARTITIONS = "w2vPartitions"
  val INPUT = "input"
  val OUTPUT = "output"
  val CMD = "cmd"
  val KRYO = "kryo"
  val PARTITIONED = "partitioned"
  val LEARNING_RATE = "lr"
  val ITERATION = "iter"
  val DIMENSION = "dim"
  val WINDOW = "window"
  val ALIAS_SAMPLING = "aliasSampling"

  private lazy val defaultParams = Params()
  private lazy val parser = new OptionParser[Params]("2nd Order Random Walk + Word2Vec") {
    head("Main")
    opt[Int](WALK_LENGTH)
      .text(s"walkLength: ${defaultParams.walkLength}")
      .action((x, c) => c.copy(walkLength = x))
    opt[Int](NUM_WALKS)
      .text(s"numWalks: ${defaultParams.numWalks}")
      .action((x, c) => c.copy(numWalks = x))
    opt[Double](P)
      .text(s"return parameter p: ${defaultParams.p}")
      .action((x, c) => c.copy(p = x))
    opt[Double](Q)
      .text(s"in-out parameter q: ${defaultParams.q}")
      .action((x, c) => c.copy(q = x))
    opt[Int](RDD_PARTITIONS)
      .text(s"Number of RDD partitions in running Random Walk and Word2vec: ${
        defaultParams
          .rddPartitions
      }")
      .action((x, c) => c.copy(rddPartitions = x))
    opt[Boolean](WEIGHTED)
      .text(s"weighted: ${defaultParams.weighted}")
      .action((x, c) => c.copy(weighted = x))
    opt[Boolean](DIRECTED)
      .text(s"directed: ${defaultParams.directed}")
      .action((x, c) => c.copy(directed = x))
    opt[Boolean](ALIAS_SAMPLING)
      .text(s"Use alias sampling (uses more memory): ${defaultParams.aliasSampling}")
      .action((x, c) => c.copy(aliasSampling = x))
    opt[Int](W2V_PARTITIONS)
      .text(s"Number of partitions in word2vec: ${defaultParams.w2vPartitions}")
      .action((x, c) => c.copy(w2vPartitions = x))
    opt[String](INPUT)
      .required()
      .text("Input edge file path: empty")
      .action((x, c) => c.copy(input = x))
    opt[String](OUTPUT)
      .required()
      .text("Output path: empty")
      .action((x, c) => c.copy(output = x))
    opt[String](CMD)
      .required()
      .text(s"command: ${defaultParams.cmd.toString}")
      .action((x, c) => c.copy(cmd = TaskName.withName(x)))
    opt[Boolean](KRYO)
      .text(s"Whether to use kryo serializer or not: ${defaultParams.useKyroSerializer}")
      .action((x, c) => c.copy(useKyroSerializer = x))
    opt[Boolean](PARTITIONED)
      .text(s"Whether the graph is partitioned: ${defaultParams.partitioned}")
      .action((x, c) => c.copy(partitioned = x))
    opt[Double](LEARNING_RATE)
      .text(s"Learning rate in word2vec: ${defaultParams.w2vLr}")
      .action((x, c) => c.copy(w2vLr = x))
    opt[Int](ITERATION)
      .text(s"Number of iterations in word2vec: ${defaultParams.w2vIter}")
      .action((x, c) => c.copy(w2vIter = x))
    opt[Int](DIMENSION)
      .text(s"Number of dimensions in word2vec: ${defaultParams.w2vDim}")
      .action((x, c) => c.copy(w2vDim = x))
    opt[Int](WINDOW)
      .text(s"Window size in word2vec: ${defaultParams.w2vWindow}")
      .action((x, c) => c.copy(w2vWindow = x))
    note(
      s"""
         |For example, to run the application you can use the following command:
         |
        | bin/spark-submit --class au.csiro.data61.randomwalk.Main --$CMD ${TaskName.node2vec}
      """.stripMargin +
        s"|   --$LEARNING_RATE ${defaultParams.w2vLr}" +
        s"|   --$ITERATION ${defaultParams.w2vIter}" +
        s"|   --$W2V_PARTITIONS ${defaultParams.w2vPartitions}" +
        s"|   --$DIMENSION ${defaultParams.w2vDim}" +
        s"|   --$WINDOW ${defaultParams.w2vWindow}" +
        s"|   --$INPUT <path>" +
        s"|   --$OUTPUT <path>"
    )
  }

  def parse(args: Array[String]) = {
    parser.parse(args, defaultParams)
  }
}
