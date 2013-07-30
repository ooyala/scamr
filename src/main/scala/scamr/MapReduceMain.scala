package scamr

import org.apache.hadoop.conf.{Configuration, Configured}
import org.apache.hadoop.util.{ToolRunner, Tool}

abstract class MapReduceMain extends Configured with Tool {
  def main(args: Array[String]): Unit = ToolRunner.run(this, args) match {
    case errorCode if errorCode != 0 => throw new RuntimeException("Failed with exit code " + errorCode)
    case _ =>
  }

  override final def run(args: Array[String]): Int = {
    val configuration = getConf
    if (configuration.getBoolean("scamr.local.mode", false)) {
      configuration.set("mapred.job.tracker", "local")
      configuration.set("fs.default.name", "file:///")
    }
    run(configuration, args)
  }

  // Subclasses must implement this method. ToolRunner will automagically parse common hadoop arguments from the input
  // args and store them in the configuration (i.e. "-D mapred.reduce.tasks=10", etc.)
  def run(conf: Configuration, args: Array[String]): Int
}
