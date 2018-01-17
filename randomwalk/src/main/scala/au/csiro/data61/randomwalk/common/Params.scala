package au.csiro.data61.randomwalk.common

import au.csiro.data61.randomwalk.common.CommandParser.TaskName
import au.csiro.data61.randomwalk.common.CommandParser.TaskName.TaskName


case class Params(walkLength: Int = 80,
                  numWalks: Int = 10,
                  weighted: Boolean = true,
                  directed: Boolean = false,
                  input: String = null,
                  output: String = null,
                  rddPartitions: Int = 200,
                  partitioned: Boolean = false,
                  nodes: String = "",
                  cmd: TaskName = TaskName.firstorder) extends AbstractParams[Params]