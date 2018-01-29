package au.csiro.data61.randomwalk.common

import scopt.OptionParser

object CommandParser {

  object TaskName extends Enumeration {
    type TaskName = Value
    val firstorder, queryPaths, probs, degrees, affecteds, passProbs = Value
  }

  val WALK_LENGTH = "walkLength"
  val NUM_WALKS = "numWalks"
  val RDD_PARTITIONS = "rddPartitions"
  val WEIGHTED = "weighted"
  val DIRECTED = "directed"
  val AL = "al"
  val INPUT = "input"
  val OUTPUT = "output"
  val CMD = "cmd"
  val NODE_IDS = "nodes"

  private lazy val defaultParams = Params()
  private lazy val parser = new OptionParser[Params]("2nd Order Random Walk + Word2Vec") {
    head("Main")
    opt[Int](WALK_LENGTH)
      .text(s"walkLength: ${defaultParams.walkLength}")
      .action((x, c) => c.copy(walkLength = x))
    opt[Int](NUM_WALKS)
      .text(s"numWalks: ${defaultParams.numWalks}")
      .action((x, c) => c.copy(numWalks = x))
    opt[Int](AL)
      .text(s"numWalks: ${defaultParams.affectedLength}")
      .action((x, c) => c.copy(affectedLength = x))
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
    opt[String](INPUT)
      .required()
      .text("Input edge file path: empty")
      .action((x, c) => c.copy(input = x))
    opt[String](OUTPUT)
      .required()
      .text("Output path: empty")
      .action((x, c) => c.copy(output = x))
    opt[String](NODE_IDS)
      .text("Node IDs to query from the paths: empty")
      .action((x, c) => c.copy(nodes = x))
    opt[String](CMD)
      .required()
      .text(s"command: ${defaultParams.cmd.toString}")
      .action((x, c) => c.copy(cmd = TaskName.withName(x)))
  }

  def parse(args: Array[String]) = {
    parser.parse(args, defaultParams)
  }
}
