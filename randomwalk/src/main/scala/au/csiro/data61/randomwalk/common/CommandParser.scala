package au.csiro.data61.randomwalk.common

import scopt.OptionParser

object CommandParser {

  object TaskName extends Enumeration {
    type TaskName = Value
    val node2vec, randomwalk, embedding = Value
  }

  private lazy val defaultParams = Params()
  private lazy val parser = new OptionParser[Params]("2nd Order Random Walk + Word2Vec") {
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
      .action((x, c) => c.copy(cmd = TaskName.withName(x)))
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

  def parse(args: Array[String]) = {
    parser.parse(args, defaultParams)
  }
}
