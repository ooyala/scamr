package scamr.io

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapreduce.lib.input.{SequenceFileInputFormat, KeyValueTextInputFormat, TextInputFormat, FileInputFormat}
import org.apache.hadoop.mapreduce.{OutputFormat, Job, InputFormat}
import org.apache.hadoop.mapreduce.lib.output.{SequenceFileOutputFormat, TextOutputFormat, FileOutputFormat}
import org.apache.hadoop.io.compress.{DefaultCodec, SnappyCodec, CompressionCodec}
import org.apache.hadoop.io.{Writable, Text, LongWritable}
import org.apache.hadoop.conf.Configuration

object InputOutput {
  // A trait for input sources
  trait Source[K, V] {
    val inputFormatClass: Class[_ <: InputFormat[K, V]]
    def configureInput(job: Job) {
      job.setInputFormatClass(inputFormatClass)
    }
  }

  // A base class for all Sources that read input from files.
  abstract class FileSource[K, V]
      (override val inputFormatClass: Class[_ <: InputFormat[K, V]], val inputDirs: Iterable[String])
      extends Source[K, V] {

    override def configureInput(job: Job) {
      super.configureInput(job)
      inputDirs.map { new Path(_) }.foreach { FileInputFormat.addInputPath(job, _) }
    }
  }

  class TextFileSource(inputDirs: Iterable[String])
      extends FileSource[LongWritable, Text](classOf[TextInputFormat], inputDirs)

  class KeyValueTextFileSource(inputDirs: Iterable[String])
      extends FileSource[Text, Text](classOf[KeyValueTextInputFormat], inputDirs)

  class SequenceFileSource[K, V](inputDirs: Iterable[String])
      extends FileSource[K, V](classOf[SequenceFileInputFormat[K, V]], inputDirs)

  // A trait for output sinks
  trait Sink[K, V] {
    def outputFormatClass: Class[_ <: OutputFormat[K, V]]
    def configureOutput(job: Job) {
      job.setOutputFormatClass(outputFormatClass)
    }
  }

  // A base class for all Sinks that write output to files.
  abstract class FileSink[K, V]
      (override val outputFormatClass: Class[_ <: OutputFormat[K, V]], val outputDir: String)
      (implicit km: Manifest[K], vm: Manifest[V])
      extends Sink[K, V] {

    // The key and value types must be Writable so they can be written to the job output files.
    // Unfortunately this check happens at runtime rather than compile time. The reason is that we can't
    // expect all MR jobs to produce Writable key/value types - some OutputFormats (for example, Cassandra)
    // take non-Writable key/value types and store them outside of HDFS using their own storage mechanism.
    // Although this check happens at runtime, at least it happens during the job configuration stage, before
    // any jobs are launched.
    mustBeWritable(km, "Key class")
    mustBeWritable(vm, "Value class")

    override def configureOutput(job: Job) {
      super.configureOutput(job)
      FileOutputFormat.setOutputPath(job, new Path(outputDir))
    }
  }

  class TextFileSink[K, V](outputDir: String)(implicit km: Manifest[K], vm: Manifest[V])
      extends FileSink[K, V](classOf[TextOutputFormat[K, V]], outputDir);

  class SequenceFileSink[K, V](outputDir: String)(implicit km: Manifest[K], vm: Manifest[V])
      extends FileSink[K, V](classOf[SequenceFileOutputFormat[K, V]], outputDir);

  // A trait for an IO object that's both a source and a sink. This is used to "glue" multi-stage MR
  // pipelines together.
  trait Link[K, V] extends Source[K, V] with Sink[K, V]

  abstract class FileLink[K, V](override val outputFormatClass: Class[_ <: OutputFormat[K, V]],
                                override val inputFormatClass: Class[_ <: InputFormat[K, V]],
                                val workingDir: String)(implicit km: Manifest[K], vm: Manifest[V])
                                extends Link[K, V] {

    // The key and value types must be Writable so they can be written to the intermediate sequence files.
    // Unfortunately this check happens at runtime rather than compile time. The reason is that we can't
    // expect all MR jobs to produce Writable key/value types - some OutputFormats (for example, Cassandra)
    // take non-Writable key/value types and store them outside of HDFS using their own storage mechanism.
    // Although this check happens at runtime, at least it happens during the job configuration stage, before
    // any jobs are launched.
    mustBeWritable(km, "Key class")
    mustBeWritable(vm, "Value class")

    override def configureOutput(producerJob: Job) {
      super.configureOutput(producerJob)
      FileOutputFormat.setOutputPath(producerJob, new Path(workingDir))

      // When using a FileLink, compress the job output with the SnappyCodec. This should make the job
      // run faster and use less disk space in basically every case, with no negative side effects.
      // However, only do so if the snappy native libraries are loaded.
      val conf = producerJob.getConfiguration
      val defaultCodec = if (SnappyCodec.isNativeSnappyLoaded(conf)) {
        // Prefer the SnappyCodec if the Snappy native libraries are loaded ...
        classOf[SnappyCodec]
      } else {
        // ... else, prefer LzoCodec if the GPL'ed hadoop-gpl-compression library is installed on our cluster,
        // and fall back to DefaultCodec if neither of Snappy or Lzo is available.
        Option(conf.getClass("io.compression.codec.lzo.class", null, classOf[CompressionCodec])) match {
          case Some(lzoCodec) => lzoCodec
          case None => classOf[DefaultCodec]
        }
      }
      // Allow the user to overwrite the codec we actually use on the command line
      val codecClass = conf.getClass("scamr.interstage.compression.codec", defaultCodec, classOf[CompressionCodec])

      if (codecClass != classOf[NullCompressionCodec]) {
        conf.setBoolean("mapred.output.compress", true)
        conf.set("mapred.output.compression.type", "BLOCK")
        conf.setClass("mapred.output.compression.codec", codecClass, classOf[CompressionCodec])
      } else {
        conf.setBoolean("mapred.output.compress", false)
      }
    }

    override def configureInput(consumerJob: Job) {
      super.configureInput(consumerJob)
      FileInputFormat.addInputPath(consumerJob, new Path(workingDir))
    }

    def cleanupWorkingDir(conf: Configuration) {
      if (!conf.getBoolean("scamr.always.keep.interstage.files", false)) {
        val path = new Path(workingDir)
        val fs = FileSystem.get(path.toUri, conf)
        fs.delete(path, true)
      }
    }
  }

  class SequenceFileLink[K, V](workingDir: String)(implicit km: Manifest[K], vm: Manifest[V])
      extends FileLink[K, V](classOf[SequenceFileOutputFormat[K, V]], classOf[SequenceFileInputFormat[K, V]], workingDir);

  private def mustBeWritable[T](manifest: Manifest[T], messagePrefix: String) {
    val clazz = manifest.erasure.asInstanceOf[Class[T]]
    if (!classOf[Writable].isAssignableFrom(clazz)) {
      throw new RuntimeException(
        "%s: %s is not a subclass of org.apache.hadoop.io.Writable!".format(messagePrefix, clazz.toString))
    }
  }
}