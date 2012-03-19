package scamr

import org.apache.hadoop.util.{ToolRunner, Tool}
import org.apache.hadoop.conf.{Configuration, Configured}

abstract class MapReduceMain extends Configured with Tool {
  def main(args: Array[String]): Int = ToolRunner.run(this, args)

  final def run(args: Array[String]): Int = run(getConf, args)

  // Subclasses must implement this method. ToolRunner will automagically parse common hadoop arguments from the input
  // args and store them in the configuration (i.e. "-D mapred.reduce.tasks=10", etc.)
  def run(conf: Configuration, args: Array[String]): Int
}