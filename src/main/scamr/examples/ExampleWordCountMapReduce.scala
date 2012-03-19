package scamr.examples

import org.apache.hadoop.conf.Configuration

import scamr.MapReduceMain
import scamr.conf.ConfigureSpeculativeExecution
import scamr.io.InputOutput
import scamr.mapreduce.{MapReduceJob, MapReducePipeline}
import scamr.conf.LambdaJobModifier

object ExampleWordCountMapReduce extends MapReduceMain {
  override def run(conf: Configuration, args: Array[String]) : Int = {
    val inputDirs = List(args(0))
    val outputDir = args(1)
    val pipeline = MapReducePipeline.init(conf) -->  // hint: start by adding a data source with -->
      new InputOutput.TextFileSource(inputDirs) -->  // hint: use --> to direct input into or out of a stage
      new MapReduceJob(mapper = classOf[WordCountMapper],
                       combiner = classOf[WordCountReducer],
                       reducer = classOf[WordCountReducer],
                       name = "ScaMR word count example") ++
      ConfigureSpeculativeExecution(false, false) ++  // hint: use ++ to add (Conf|Job)Modifiers to a TaskStage
      LambdaJobModifier { _.setNumReduceTasks(2) } -->
      // hint: wrapping the Sink constructor in a 0-ary closure means you don't need to specify k/v types.
      // you could do this instead, but then you have to specify the k/v types. I'm working on a cleaner way
      // to get scalac to do the type inference ...
      //   new InputOutput.TextFileSink[Text, LongWritable](outputDir)
      { () => new InputOutput.TextFileSink(outputDir) }
    return if (pipeline.execute) 0 else 1
 }
}