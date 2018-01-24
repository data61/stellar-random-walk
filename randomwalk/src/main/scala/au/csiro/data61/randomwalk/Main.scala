package au.csiro.data61.randomwalk

import au.csiro.data61.randomwalk.algorithm.UniformRandomWalk
import au.csiro.data61.randomwalk.common.CommandParser.TaskName
import au.csiro.data61.randomwalk.common.{CommandParser, Params}
import org.apache.log4j.LogManager
import org.apache.spark.{SparkConf, SparkContext}

object Main {
  lazy val logger = LogManager.getLogger("myLogger")

  def main(args: Array[String]) {
    CommandParser.parse(args) match {
      case Some(params) =>
        logger.info(params.nodes)
        val conf = new SparkConf().setAppName("Node2Vec")
        val context: SparkContext = new SparkContext(conf)
        execute(context, params)
      case None => sys.exit(1)
    }
  }

  def execute(context: SparkContext, params: Params): Unit = {
    val rw = UniformRandomWalk(context, params)
    val paths = params.cmd match {
      case TaskName.firstorder =>
        val g = rw.loadGraph()
        rw.save(rw.firstOrderWalk(g))
      case TaskName.queryPaths =>
        context.textFile(params.input).repartition(params.rddPartitions).
          map(_.split("\\s+").map(s => s.toInt))
      case TaskName.probs =>
        val g = rw.loadGraph()
        rw.save(rw.firstOrderWalk(g))
    }

    params.cmd match {
      case TaskName.queryPaths =>
        val counts = rw.queryPaths(paths)
        println(s"Total counts: ${counts.length}")
        rw.save(counts)
      case TaskName.probs =>
        val probs = rw.computeProbs(paths)
        println(s"Total prob entries: ${probs.length}")
        rw.save(probs)
    }
  }
}
