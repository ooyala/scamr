package scamr.examples

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer
import scamr.MapReduceMain
import scamr.conf.ConfigureSpeculativeExecution
import scamr.conf.LambdaJobModifier
import scamr.io.InputOutput
import scamr.mapreduce.{MapReducePipeline, MapReduceJob}


/*
 * This example demonstrates compatibility with pure-hadoop reducers, by using a pure-hadoop reducer that ships with
 * the hadoop distribution. Well actually we test a scala subclass of it because the reducer is parameterized, but
 * that's fine - it's still not an instance of ScaMR's SimpleReducer or a lambda.
 */
object ExampleWordCountWithHadoopReducer extends MapReduceMain {
  class HadoopWordCountReducer extends LongSumReducer[Text];

  override def run(conf: Configuration, args: Array[String]): Int = {
    val inputDirs = List(args(0))
    val outputDir = args(1)
    val pipeline = MapReducePipeline.init(conf) -->  // hint: start by adding a data source with -->
      new InputOutput.TextFileSource(inputDirs) -->  // hint: use --> to direct input into or out of a stage
      new MapReduceJob(classOf[WordCountMapper], new HadoopWordCountReducer(), "ScaMR word count example") ++
      ConfigureSpeculativeExecution(false, false) ++  // hint: use ++ to add (Conf|Job)Modifiers to a TaskStage
      LambdaJobModifier { _.setNumReduceTasks(1) } -->
      new InputOutput.TextFileSink[Text, LongWritable](outputDir)
    return if (pipeline.execute) 0 else 1
  }
}