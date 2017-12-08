package au.csiro.data61.randomwalk.common

import au.csiro.data61.randomwalk.common.CommandParser.TaskName
import au.csiro.data61.randomwalk.common.CommandParser.TaskName.TaskName


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
                  cmd: TaskName = TaskName.node2vec) extends AbstractParams[Params]