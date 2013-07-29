package scamr.examples

import org.apache.hadoop.conf.Configuration
import scamr.MapReduceMain
import scamr.conf.ConfigureSpeculativeExecution
import scamr.conf.LambdaJobModifier
import scamr.io.InputOutput
import scamr.io.lib.CombineTextFileSource
import scamr.mapreduce.{MapReduceJob, MapReducePipeline}

object ExampleWordCountMapReduce extends MapReduceMain {
  override def run(conf: Configuration, args: Array[String]): Int = {
    val inputDirs = args.init
    val outputDir = args.last
    val combineInputFiles = conf.getBoolean("combine.input.files", false)
    val pipeline = MapReducePipeline.init(conf) -->  // hint: start by adding a data source with -->
      // hint: use --> to direct input into or out of a stage
      (if (combineInputFiles) new CombineTextFileSource(inputDirs) else new InputOutput.TextFileSource(inputDirs)) -->
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
      (() => new InputOutput.TextFileSink(outputDir))
    if (pipeline.execute) 0 else 1
 }
}