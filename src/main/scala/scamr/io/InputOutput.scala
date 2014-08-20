package scamr.io

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.compress.{DefaultCodec, SnappyCodec, CompressionCodec}
import org.apache.hadoop.io.{Writable, Text, LongWritable}
import org.apache.hadoop.mapreduce.lib.input.{SequenceFileInputFormat, KeyValueTextInputFormat, TextInputFormat}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output._
import org.apache.hadoop.mapreduce.{OutputFormat, Job, InputFormat}
import scamr.conf.HadoopVersionSpecific

object InputOutput {
  // A trait for input sources
  trait Source[K, V] {
    val inputFormatClass: Class[_ <: InputFormat[K, V]]
    def configureInput(job: Job) {
      job.setInputFormatClass(inputFormatClass)
    }

    // Subclasses can override to run custom code after the input has been read from this Source, or the stage reading
    // the input failed.
    def onInputRead(job: Job, success: Boolean) {}
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

    // Subclasses can override to run custom code after the output has been written to this Sink successfully, or
    // the stage writing to the output failed.
    def onOutputWritten(job: Job, success: Boolean) {}
  }

  // A sink that discards all output written to it.
  class NullSink[K, V] extends InputOutput.Sink[K, V] {
    def outputFormatClass: Class[_ <: OutputFormat[K, V]] = classOf[NullOutputFormat[K, V]]
  }

  // A base class for all Sinks that write output to files.
  abstract class FileSink[K, V]
  (override val outputFormatClass: Class[_ <: OutputFormat[K, V]], val outputDir: Path)
  (implicit km: Manifest[K], vm: Manifest[V])
  extends Sink[K, V] {

    def this(outputFormatClass: Class[_ <: OutputFormat[K, V]], outputDir: String)
            (implicit km: Manifest[K], vm: Manifest[V]) = this(outputFormatClass, new Path(outputDir))(km, vm)

    // The key and value types must be Writable so they can be written to the job output files.
    // Unfortunately this check happens at runtime rather than compile time. The reason is that we can't
    // expect all MR jobs to produce Writable key/value types - some OutputFormats (for example, Cassandra)
    // take non-Writable key/value types and store them outside of HDFS using their own storage mechanism.
    // Although this check happens at runtime, at least it happens during the job configuration stage, before
    // any jobs are launched.
    // mustBeWritable(km, "Key class")
    // mustBeWritable(vm, "Value class")

    override def configureOutput(job: Job) {
      super.configureOutput(job)
      FileOutputFormat.setOutputPath(job, outputDir)
    }
  }

  class TextFileSink[K, V](outputDir: Path)(implicit km: Manifest[K], vm: Manifest[V])
  extends FileSink[K, V](classOf[TextOutputFormat[K, V]], outputDir) {

    def this(outputDir: String)(implicit km: Manifest[K], vm: Manifest[V]) = this(new Path(outputDir))(km, vm)
  }

  class SequenceFileSink[K, V](outputDir: Path)(implicit km: Manifest[K], vm: Manifest[V])
  extends FileSink[K, V](classOf[SequenceFileOutputFormat[K, V]], outputDir) {

    def this(outputDir: String)(implicit km: Manifest[K], vm: Manifest[V]) = this(new Path(outputDir))(km, vm)
  }

  // A trait for an IO object that's both a source and a sink. This is used to "glue" multi-stage MR
  // pipelines together.
  trait Link[K, V] extends Source[K, V] with Sink[K, V]

  abstract class FileLink[K, V](override val outputFormatClass: Class[_ <: OutputFormat[K, V]],
                                override val inputFormatClass: Class[_ <: InputFormat[K, V]],
                                val workingDir: Path)
                               (implicit km: Manifest[K], vm: Manifest[V])
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
      FileOutputFormat.setOutputPath(producerJob, workingDir)

      // When using a FileLink, compress the job output with the SnappyCodec. This should make the job
      // run faster and use less disk space in basically every case, with no negative side effects.
      // However, only do so if the snappy native libraries are loaded.
      val conf = producerJob.getConfiguration
      val defaultCodec = if (HadoopVersionSpecific.isNativeSnappyLoaded(conf)) {
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
        conf.setBoolean(HadoopVersionSpecific.ConfKeys.CompressOutput, true)
        conf.set(HadoopVersionSpecific.ConfKeys.OutputCompressionType, "BLOCK")
        conf.setClass(HadoopVersionSpecific.ConfKeys.OutputCompressionCodec, codecClass, classOf[CompressionCodec])
      } else {
        conf.setBoolean(HadoopVersionSpecific.ConfKeys.CompressOutput, false)
      }
    }

    override def configureInput(consumerJob: Job) {
      super.configureInput(consumerJob)
      FileInputFormat.addInputPath(consumerJob, workingDir)
    }

    // Subclasses can override to run custom code after the input has been read from the Source successfully.
    override def onInputRead(job: Job, success: Boolean) {
      super.onInputRead(job, success)
      if (success) { cleanupWorkingDir(job.getConfiguration) }
    }

    def cleanupWorkingDir(conf: Configuration) {
      if (!conf.getBoolean("scamr.always.keep.interstage.files", false)) {
        FileSystem.get(workingDir.toUri, conf).delete(workingDir, true)
      }
    }
  }

  class SequenceFileLink[K, V](workingDir: Path)(implicit km: Manifest[K], vm: Manifest[V])
  extends FileLink[K, V](classOf[SequenceFileOutputFormat[K, V]], classOf[SequenceFileInputFormat[K, V]], workingDir)

  def mustBeWritable[T](manifest: Manifest[T], messagePrefix: String) {
    val clazz = manifest.erasure.asInstanceOf[Class[T]]
    if (!classOf[Writable].isAssignableFrom(clazz)) {
      throw new RuntimeException(
        "%s: %s is not a subclass of org.apache.hadoop.io.Writable!".format(messagePrefix, clazz.toString))
    }
  }
}
