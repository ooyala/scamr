# ScaMR

ScaMR (pronounced "scammer") is a Scala framework for writing Hadoop MapReduce jobs and/or pipelines.

This project was heavily influenced by [ScalaHadoop](https://github.com/bsdfish/ScalaHadoop "ScalaHadoop"), which seems to not be maintained at this point.

The LambdaMapper / LambdaReducer code was heavily influenced by Jonathan Clark's [Scadoop project](https://github.com/jhclark/scadoop "scadoop").

ScaMR has been tested with, and compiles against, [Scala 2.9.1 final](http://www.scala-lang.org/node/10780 "Scala 2.9.1.final") and [Cloudera's CDH3u1 hadoop distribution](http://www.cloudera.com/blog/2011/07/cdh3u1-released/ "Cloudera's CDH3u1").

# Overview

ScaMR is a Scala framework for writing Hadoop MapReduce jobs and/or pipelines. Key points:

* Uses the native Hadoop API rather than Hadoop Streaming. This is faster, more flexible, and more powerful than the streaming alternative.
* Only the new `org.apache.hadoop.mapreduce` API is supported (for now), not the deprecated `org.apache.hadoop.mapred` API.
* Has full support for using existing Java Mappers / Reducers in your MR jobs or pipelines, allowing for code reuse and incremental upgrade.
* Provides its own SimpleMapper / SimpleReducer API which is somewhat simpler and more concise than the underlying Hadoop API. The main difference is that the context is passed into the constructor, so there is no need for a `setup()` method, and there are nice-looking helper methods to update counters and emit key/value pairs.
* Also supports lambda mappers / reducers. All you do is provide a lambda function that is the body of your map() / reduce().
* Has a simple syntax for specifying arbitrary configuration modifiers for your MR jobs.
* Assumes that multi-job pipelines are the common case and optimizes for it. Stand-alone jobs are just specified as one-stage pipelines.
* Uses Snappy-compressed SequenceFiles for piping data from one job to the next (NOTE: This requires the SnappyCodec to be installed on your cluster. Cloudera's CDH3u1+ provides this out of the box, but your mileage may vary with other Hadoop distributions).
* For now, only supports linear multi-job pipelines, but may well support DAGs (for running independent stages in parallel) in the future.
* (Since version 0.2.0) Supports dependency injection into mappers, combiners, and reducers using [SubCut](https://github.com/dickwall/subcut).

# MapReduce job pipelines

The ScaMR framework is pipeline-centric. Every MapReduce job or sequential chain of jobs constitutes a pipeline. The general structure of a pipeline is:

`Input source --> MR job [++ Conf/job modifier]* [--> MR job [++ Conf/job modifier]*]* --> Output sink`

Or, in plain English: A pipeline is an input source, followed by one or more MapReduce jobs, each of which has zero or more conf/job modifiers, followed by an output sink. To start defining a pipeline, create a scala `object` that extends `scamr.MapReduceMain`, override the `run(Configuration, Array[String]): Int` method, and inside the run method instantiate a pipeline with `MapReducePipeline.init(Configuration)`, like this:

```scala
object MyMapReduce extends scamr.MapReduceMain {
  override def run(baseHadoopConfiguration: Configuration, args: Array[String]): Int = {
    // ...
    val pipeline = scamr.mapreduce.MapReducePipeline.init(baseHadoopConfiguration)
    // ...
  }
}
```

Note that any properties you set on the command line with `-D property.name=property.value` will be parsed appropriately and set in your `baseHadoopConfiguration` before the `run()` method is called. For this reason, you should *probably* always use the `baseHadoopConfiguration` instead of creating a new one from scratch (that is, unless you really know what you're doing).

## Input Sources

Every MapReduce pipeline needs some input data to process. Input data is specified with an input Source, the code for which is in `src/main/scamr/io/InputOutput.scala`. Several input Sources are provided out of the box, and you can write your own Sources by extending the `scamr.io.InputOutput.Source` trait. To specify a source which processes text files and breaks them up into lines:

```scala
val inputs = List("/logs.production/2011/01/01/*", "/logs.production/2011/01/02/*")
val pipeline = MapReducePipeline.init(baseHadoopConfiguration) -->
               new InputOutput.TextFileSource(inputs)
```

Notice that multiple input paths can be specified as arguments to the constructor of an InputOutput.TextFileSource object, but only a single Source object can be specified per pipeline. Joining inputs from multiple source types may be supported in a future version.

Once you've specified some input to a pipeline, you probably want to run it through one or more MapReduce jobs. Use the `-->` operator to direct the flow of data through your pipeline: from an input Source, into an MR job, between successive stages of your multi-job pipeline, and finally into an output Sink.

## MapReduce jobs

A MapReduce job is specified by instantiating a `MapReduceJob` or `MapOnlyJob` object. A `MapReduceJob` must have both a map stage and a reduce stage, with an optional combiner. A `MapOnlyJob` only has a map stage. Both types of jobs must also have a name. To specify your mapper/reducer/combiner, you use a `MapperDef` / `ReducerDef` / `CombinerDef` class. However, thanks to implicit conversions, you never need to instantiate one of these `*Def` objects directly.

Let's see a simple example, which we will take apart in the next few sections:

```scala
class MyMapper(context: MapContext[_,_,_,_]) extends SimpleMapper[K1, V1, K2, V2](context) { ... };
class MyCombiner(context: ReduceContext[_,_,_,_]) extends SimpleCombiner[K2, V2](context) { ... };
class MyReducer(context: ReduceContext[_,_,_,_]) extends SimpleReducer[K2, V2, K3, V3](context) { ... };

val inputs = List("/logs.production/2011/01/01/*", "/logs.production/2011/01/02/*")
val pipeline = MapReducePipeline.init(baseHadoopConfiguration) -->
               new InputOutput.TextFileSource(inputs) -->
               new MapReduceJob(classOf[MyMapper], classOf[MyCombiner], classOf[MyReducer], "My MR job") -->
               ...
```

A map-only job can be specified by using `MapReduceJob` and supplying the mapper and job name, like this:

```scala
... --> new MapOnlyJob(classOf[MyMapper], "My map-only job") --> ...
```

Unless you are running a map-only job, compression of map outputs using the Snappy codec is enabled by default. This means that your cluster must have Snappy compression support installed (Cloudera's CDH3u1 or later does by default). You can disable this behavior completely by adding `-D scamr.interstage.compression.codec=scamr.io.NullCompressionCodec` to your MR job command. Or, you can change the type of compression codec to something other than `SnappyCodec` by specifying a different codec class name.

## Defining Mappers / Reducers / Combiners

There are 3 different ways to specify a Mapper / Reducer / Combiner in ScaMR. These are:

* Java Hadoop API
* ScaMR SimpleMapper / SimpleReducer API
* Lambda functions

Let's look at these one at a time.

### Hadoop API

ScaMR has full support for integrating your existing Java mappers / reducers / combiners into your Scala MR job pipeline. To do so, create a *new instance* of the Java mapper / reducer / combiner and pass it to your `MapReduceJob()` or `MapOnlyJob()` constructor. Note that implicit conversions will automagically create the appropriate type of `MapperDef` / `ReducerDef` / `CombinerDef` object for you.

Hadoop already requires that all such mappers / reducers / combiners have a zero-argument constructor and do all of their configuration in the `setup()` method, so assuming your mapper/reducer/combiner is written properly, it should be possible to instantiate it on the machine that's launching the job without specifying any arguments or doing any configuration.

Obligatory example:

```java
// This is your old, crusty Java code
public class MyLegacyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
  // ...
}

public class MyLegacyReducer extends Mapper<Text, LongWritable, Text, LongWritable> {
  // ...
}
```

```scala
// This is your new, shiny Scala code
val inputs = List("/logs.production/2011/01/01/*", "/logs.production/2011/01/02/*")
val pipeline = MapReducePipeline.init(baseHadoopConfiguration) -->
               new InputOutput.TextFileSource(inputs) -->
               new MapReduceJob(new MyLegacyMapper(), new MyLegacyReducer(), "My legacy MR job") --> ...
```

### ScaMR Simple(Mapper|Reducer|Combiner) API

ScaMR also provides an alternative API for writing a Mapper / Reducer / Combiner class. The base classes are named `SimpleMapper` / `SimpleReducer` / `SimpleCombiner`. There are a few key differences between these classes and their Hadoop API counterparts:

* There is no `setup()` method. Rather, the context is passed to the primary constructor. This allows initial state to be extracted from the context in the constructor body and stored in `vals` rather than `vars`, which is better Scala style.
* The `map()`, `reduce()`, and `cleanup()` methods don't take a `context` argument, because the context never changes between calls to these methods. Instead, the initial context is used, and knowledge of this is hidden inside the framework code.
* Counter updates are performed with the `updateCounter()` methods, which can be found in `src/main/scamr/mapreduce/CounterUpdater.scala`.
* Producing key/value pairs is performed with the `emit()` methods, which can be found in
`src/main/scamr/mapreduce/KeyValueEmitter.scala`.
* `SimpleCombiner` is just an abstract subclass of `SimpleReducer` where the input key/value types and the output key/value types are the same, as must be the case for all combiners. Any `SimpleReducer` that meets this requirement can be used as a combiner.
* To specify a mapper / reducer / combiner that uses ScaMR's simple API, pass the *class instance* to your `MapReduceJob()` or `MapOnlyJob()` constructor. The class instance is used because these classes don't have zero-argument constructors, and thus cannot be instantiated on the machine that's launching the job before the full job configuration is known.

Obligatory example:

```scala
class TextIdentityMapper(context: MapContext[_,_,_,_])
    extends SimpleMapper[Text, Text, Text, Text](context) {
  val conf = context.getConfiguration
  val inputSplit = context.getInputSplit
  val taskAttemptId = context.getTaskAttemptID()
  val shortClassName = this.getClass.toString.split("\\.").last
  val count = updateCounter(shortClassName, _, _)
  val logger = LoggerFactory.getLogger(this.getClass)

  logger.info("Task attempt id: %s, processing input split: %s".format(taskAttemptId.toString(),
              inputSplit.toString())

  override def map(key: Text, value: Text) {
    count("key-value pairs", 1L)
    emit(key, value)
  }
}

val inputs = List("/logs.production/2011/01/01/*", "/logs.production/2011/01/02/*")
val pipeline = MapReducePipeline.init(baseHadoopConfiguration) -->
               new InputOutput.TextFileSource(inputs) -->
               new MapOnlyJob(classOf[TextIdentityMapper], "Identity map-only job") --> ...
```

### ScaMR Lambda(Mapper|Reducer|Combiner) API

Finally, ScaMR provides an API for specifying a mapper / reducer / combiner with a lambda function which defines the
behavior of the `map()` or `reduce()`. This is the most functional and least Java-like approach available. It also
only works for tasks which need no explicit setup or cleanup code. A mapper lambda must have the signature:

```scala
val mapLambda: (Iterator[(K1, V1)], scamr.mapreduce.lambda.LambdaMapContext) => Iterator[(K2, V2)]
```

where `K1, V1` are the mapper input key/value types and `K2, V2` are the mapper output key/value types.

A reducer lambda must have the signature:

```scala
val reduceLambda: (Iterator[(K2, Iterator[V2])], scamr.mapreduce.lambda.LambdaReduceContext) => Iterator[(K3, V3)]
```

where `K2, V2` are the reducer input key/value types and `K3, V3` are the reducer output key/value types.

A combiner lambda looks like a reducer lambda, with the constraint that `K2 == K3` and `V2 == V3`.

A word count example using this functional lambda style is below:

```scala
def map(input: Iterator[(LongWritable, Text)], ctx: LambdaMapContext): Iterator[(Text, LongWritable)] =
  for {
    (offset, line) <- input;
    word <- line.toString.split("\\s+").filterNot(_.isEmpty).toIterator
  } yield (new Text(word), new LongWritable(1L))

def reduce(input: Iterator[(Text, Iterator[LongWritable])], ctx: LambdaReduceContext): Iterator[(Text, LongWritable)] =
  for {
    (word, counts) <- input
  } yield (word, new LongWritable(counts.foldLeft(0L)((a, b) => a + b.get)))

val inputs = List("/logs.production/2011/01/01/*", "/logs.production/2011/01/02/*")
val pipeline = MapReducePipeline.init(baseHadoopConfiguration) -->
               new InputOutput.TextFileSource(inputs) -->
               new MapReduceJob(map _, reduce _, "Lambda word count example") --> ...
```

## Modifying MR job configurations

The native Hadoop APIs allow the user to configure and fine-tune many job parameters. This ability is fully preserved in the ScaMR framework, through the use of `ConfModifier`s and `JobModifier`s. These can be "appended" to a job using the `++` operator, and modify the preceding MR job. Applying modifiers to Sources or Sinks is not supported, since the Source / Sink itself is just syntactic sugar for modifying the configuration of the job following it (for Sources) or preceding it (for Sinks). Let's see an example that sets the number of reduce tasks for a job, and sets a hadoop property called "my.config.param":

```scala
val inputs = List("/logs.production/2011/01/01/*", "/logs.production/2011/01/02/*")
val pipeline = MapReducePipeline.init(baseHadoopConfiguration) -->
               new InputOutput.TextFileSource(inputs) -->
               new MapReduceJob(map _, reduce _, "Job name") ++
               LambdaJobModifier { job => job.setNumReduceTasks(1) } ++
               LambdaConfModifier { conf => conf.set("my.config.param", "some.string.value") } --> ...
```

The code lives in `src/main/scamr/conf`. There are two base traits - `JobModifier` (which modifies a `org.apache.hadoop.mapreduce.Job`) and `ConfModifier` (which modifies a `org.apache.hadoop.conf.Configuration`). There are a few basic modifiers that ship with the framework that allow for things like configuring speculative execution or setting configuration parameters. There are also `LambdaConfModifier` and `LambdaJobModifier` which allow the user to specify an arbitrary lambda to apply to the `Configuration` or `Job`. You can also write your own custom modifiers which extend the `ConfModifier` or `JobModifier` traits if you like.

## Chaining MR jobs together

In our experience, most real-world applications of MapReduce require multiple jobs to actually compute something interesting. ScaMR has been designed with this in mind, and makes it extremely simple to create multi-job pipelines. To chain jobs together, just connect them with the `-->` operator:

```scala
val pipeline = MapReducePipeline.init(baseHadoopConfiguration) -->
               new InputOutput.TextFileSource(inputs) -->
               new MapReduceJob(map1 _, reduce1 _, "Stage 1") -->
               new MapReduceJob(map2 _, reduce2 _, "Stage 2") -->
               new MapReduceJob(map3 _, reduce3 _, "Stage 3") --> ...
```

Note that for all _N_, the output key/value types of stage _N_ must match the input key/value types of stage _N+1_.

Chaining is currently implemented by writing the output of intermediate jobs to a randomly-named directory, using Snappy-compressed SequenceFiles. This means that the key and value output types of any intermediate job must implement `org.apache.hadoop.io.Writable`. It also means that your cluster must support Snappy compression (which it does if you are using Cloudera's CDH3u1 or later distribution). You can disable the compression of temporary inter-job files by passing the `-D scamr.intermediate.compression.codec=scamr.io.NullCompressionCodec` argument to your job, or you can use a non-snappy `CompressionCodec` by passing in that codec's fully-qualified class name.

## Output Sinks

Finally, once you've processed your input data through one or more MR jobs, you will want to do something with the
output. To do so, you specify an output Sink, the code for which is in `src/main/scamr/io/InputOutput.scala`. Several
output Sinks are provided out of the box, and you can write your own Sinks by extending the `scamr.io.InputOutput.Sink`
trait. Obligatory example:

```scala
val inputs = List("/logs.production/2011/01/01/*", "/logs.production/2011/01/02/*")
val outputDir = "/user/my_user_name/job_output"
val pipeline = MapReducePipeline.init(baseHadoopConfiguration) -->
               new InputOutput.TextFileSource(inputs) -->
               new MapReduceJob(map1 _, reduce1 _, "Stage 1") -->
               new MapReduceJob(map2 _, reduce2 _, "Stage 2") -->
               new MapReduceJob(map3 _, reduce3 _, "Stage 3") -->
               new InputOutput.TextFileSink[Text, LongWritable](outputDir)
```

## Executing a pipeline

Once you've specified your pipeline, simply call `pipeline.execute()`. The method returns `true` if the job succeeded,
or `false` if it failed (Note: this API is not yet stable, and may throw exceptions on failures in some future version).

# Dependency Injection (since version 0.2.0)

As of version 0.2.0, ScaMR supports dependency injection into SimpleMapper / SimpleCombiner / SimpleReducer instances using [SubCut](https://github.com/dickwall/subcut). This can make it much easier to unit test complex mappers/reducers/combiners (for example with [Apache's MRUnit](http://mrunit.apache.org)), by injecting code into your mappers etc in your test cases. To make your SimpleMapper (Combiner, Reducer) use dependency injection, just a few simple steps are required:

1. Extend the trait `com.escalatesoft.subcut.inject.Injectable`
2. Add an implicit constructor parameter `(implicit override val bindingModule: BindingModule)`
3. Create a binding module which defines your bindings as a standalone scala `object`. Important - the binding module object must not be nested inside a `class`! (though, it can *probably* be nested inside another `object`).
4. Have your binding module be available as an implicit value in the place where you define your `MapReducePipeline`.
5. The framework will inject all the bindings you define in the binding module, as well as the `context` and Hadoop `Configuration` given to the mapper/combiner/reducer at runtime!

Example usage, where we create a modified WordCountMapper which uses an injected string to define the regular expression we use to split lines into words:

```scala
object SplitRegexId extends BindingId

class WordCountMapper(context: MapContext[_, _, _, _])(implicit override val bindingModule: BindingModule)
      extends SimpleMapper[LongWritable, Text, Text, LongWritable](context) with Injectable {

  val splitRegex = injectOptional [String](SplitRegexId) getOrElse { """\s+""" }

  override def map(offset: LongWritable, line: Text) =
    line.toString.split(splitRegex).foreach { word => if (!word.isEmpty) emit(new Text(word), new LongWritable(1L)) }
}

object ProdBindingModule extends NewBindingModule(module => {
  // replace the default split regex with one that removes punctuation
  module.bind[String] idBy SplitRegexId toSingle """[\s.,;:?!]+"""
})

object WordCountMapReduce extends MapReduceMain {
  implicit val bindingModule = ProdBindingModule  // required, the implicit module must be in scope when defining the pipeline
  // … create the MR pipeline as normal
}
```

To learn more about using SubCut, check out their [Getting Started](https://github.com/dickwall/subcut/blob/master/GettingStarted.md) page.

# Building

First, you need sbt 0.12.1 and scala 2.9.1 or scala 2.10.0. To build scamr as a library and install it to your local ivy cache, run:

```bash
sbt package && sbt publish-local
```

To build a "fat jar" that you can use to run the examples, run `sbt assembly`. This will create a self-contained jar that can be run locally or on a hadoop cluster at `target/scamr-examples.jar`.

TODO: Add a list of necessary SBT plugins etc.

# Running the examples

There are a few simple examples provided in `src/main/scamr/examples`. To run them, build the project, upload some text file(s) to HDFS, then run:

```bash
hadoop jar target/scamr-examples.jar scamr.examples.ExampleWordCountMapReduce <input path> <output dir>

hadoop jar target/scamr-examples.jar scamr.examples.ExampleSortedWordCountMapReduce <input path> <output dir>

hadoop jar target/scamr-examples.jar scamr.examples.ExampleLambdaWordCountMapReduce <input path> <output dir>

hadoop jar target/scamr-examples.jar scamr.examples.ExampleMapOnlyJob <input path> <output dir>
```

# Testing

There aren't really any unit or integration tests at this point, though the examples can sort of function as tests. If you feel like contributing and want to write some test code for the framework, feel free to do so!

# Contributing

ScaMR is an open-source project licensed under the Apache 2.0 license (see the LICENSE file for the legalese).
Feel free to use, modify, extend, copy, rewrite, etc any or all of this code as you see fit. Outside contributions, improvements, bug fixes, etc are always welcome, assuming of course that they pass our quality bar.
