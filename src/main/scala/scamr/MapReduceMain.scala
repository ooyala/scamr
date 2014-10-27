package scamr

import org.apache.hadoop.conf.{Configuration, Configured}
import org.apache.hadoop.util.{ToolRunner, Tool}
import org.apache.log4j.Logger
import scamr.conf.HadoopVersionSpecific

abstract class MapReduceMain extends Configured with Tool {
  val logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = try {
    ToolRunner.run(this, args) match {
      case errorCode if errorCode != 0 =>
        logger.error("failed with error code: " + errorCode)
        System.exit(errorCode)
      case _ => System.exit(0)
    }
  }

  override final def run(args: Array[String]): Int = {
    val configuration = getConf
    val isLocalMode = configuration.getBoolean("scamr.local.mode", false)
    if (isLocalMode) {
      configuration.set("mapred.job.tracker", "local")
      configuration.set(HadoopVersionSpecific.ConfKeys.DefaultFilesystem, "file:///")
      configuration.set("mapreduce.framework.name", "local")
    }
    run(configuration, args)
  }

  // Subclasses must implement this method. ToolRunner will automagically parse common hadoop arguments from the input
  // args and store them in the configuration (i.e. "-D mapred.reduce.tasks=10", etc.)
  def run(conf: Configuration, args: Array[String]): Int
}
