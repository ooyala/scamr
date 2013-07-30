package scamr.examples

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.map.TokenCounterMapper
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer
import scamr.MapReduceMain
import scamr.conf.ConfigureSpeculativeExecution
import scamr.conf.LambdaJobModifier
import scamr.io.InputOutput
import scamr.mapreduce.{MapReducePipeline, MapReduceJob}


/*
 * This example demonstrates compatibility with Java API mappers and reducers (i.e. 'vanilla' Hadoop),
 * by using a mapper and reducer which ship with the hadoop distribution.
 */
object ExampleWordCountWithHadoopReducer extends MapReduceMain {
  override def run(conf: Configuration, args: Array[String]): Int = {
    val inputDirs = List(args(0))
    val outputDir = args(1)
    val pipeline = MapReducePipeline.init(conf) -->  // hint: start by adding a data source with -->
      new InputOutput.TextFileSource(inputDirs) -->  // hint: use --> to direct input into or out of a stage
      new MapReduceJob(new TokenCounterMapper, new IntSumReducer[Text], new IntSumReducer[Text],
        "ScaMR word count example - Java API") ++
      ConfigureSpeculativeExecution(false, false) ++  // hint: use ++ to add (Conf|Job)Modifiers to a TaskStage
      LambdaJobModifier { _.setNumReduceTasks(1) } -->
      (() => new InputOutput.TextFileSink(outputDir))
    if (pipeline.execute) 0 else 1
  }
}
