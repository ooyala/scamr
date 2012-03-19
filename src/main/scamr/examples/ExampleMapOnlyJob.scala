package scamr.examples

import scamr.MapReduceMain
import org.apache.hadoop.conf.Configuration
import scamr.io.InputOutput
import scamr.conf.ConfigureSpeculativeExecution
import org.apache.hadoop.io.{LongWritable, Text}
import scamr.mapreduce.{MapOnlyJob, MapReducePipeline}

/* This example demonstrates a map-only MR job. */
object ExampleMapOnlyJob extends MapReduceMain {
  override def run(conf: Configuration, args: Array[String]) : Int = {
    val inputDirs = List(args(0))
    val outputDir = args(1)
    val pipeline = MapReducePipeline.init(conf) -->  // hint: start by adding a data source with -->
      new InputOutput.TextFileSource(inputDirs) -->  // hint: use --> to direct input into or out of a stage
      // hint: use 'new MapOnlyTask()' instead of 'new MapReduceTask()' to specify a map-only job
      new MapOnlyJob(classOf[WordCountMapper], "ScaMR map-only job example") ++
      ConfigureSpeculativeExecution(false, false) -->
      new InputOutput.TextFileSink[Text, LongWritable](outputDir)
    return if (pipeline.execute) 0 else 1
  }
}
