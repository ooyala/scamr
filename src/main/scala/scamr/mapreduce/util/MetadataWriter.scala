package scamr.mapreduce.util

import java.io.OutputStream

import com.lambdaworks.jacks.JacksMapper
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Counter, CounterGroup, Job}
import org.apache.log4j.Logger
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat
import scamr.conf.OnJobSuccess

import scala.collection.mutable

/** Writes job metadata as json in the hidden file _JobData in the output directory
  *
  * Job metadata includes jobId, jobName, startTime, finishTime, counters.
  * Additional job metadata can be provided in the Hadoop configuration "scamr.metadata.provided"
  * as a json string.
  */
object MetadataWriter {
  val ProvidedMetadataConfKey = "scamr.metadata.provided"

  import scala.collection.JavaConversions._

  val logger = Logger.getLogger(this.getClass)

  /** Callback for MetadataWriter
    *
    * To use: new MapReduceJob(...) ++ MetadataWriter.callback
    */
  val callback = OnJobSuccess { job =>
    writeMetadata(job)
  }

  /** Write metadata for Job job */
  def writeMetadata(job: Job) {
    var outputStreamOption: Option[OutputStream] = None
    try {
      // Create OutputStream to HDFS file
      val outputDir = job.getConfiguration.get(FileOutputFormat.OUTDIR)
      val resultFile = new Path(outputDir, "_JobData")
      val fs = FileSystem.get(job.getConfiguration)
      outputStreamOption = Some(fs.create(resultFile))

      // read provided metadata
      val providedDataMap =
        Option(job.getConfiguration.get(ProvidedMetadataConfKey)) match {
          case Some(providedMetadata) =>
            try {
              JacksMapper.readValue[Map[Any, Any]](providedMetadata)
            } catch {
              case e: Throwable =>
                logger.warn(s"Error parsing provided metadata [$providedMetadata], ignored", e)
                Map[Any, Any]()
            }
          case _ => Map[Any, Any]()
        }

      // Create counter map
      val counters = job.getCounters
      val counterMap = mutable.Map[String, Long]()
      for (group: CounterGroup <- counters; counter: Counter <- group) {
        counterMap += counter.getDisplayName -> counter.getValue
      }

      // write output to OutputStream
      val output = Map(
        "jobId" -> job.getJobID.toString,
        "jobName" -> job.getJobName,
        "startTime" -> new DateTime(job.getStartTime).toString(ISODateTimeFormat.dateHourMinuteSecond()),
        "finishTime" -> new DateTime(job.getFinishTime).toString(ISODateTimeFormat.dateHourMinuteSecond()),
        "counters" -> counterMap,
        "provided" -> providedDataMap
      )
      outputStreamOption.get.write(JacksMapper.writeValueAsString(output).getBytes)

    } catch {
      case x: Throwable =>
        logger.warn(s"Failed to write job metadata file to output location.", x)

    } finally {
      // Ensure OutputStream is closed
      outputStreamOption.foreach(_.close())
    }
  }

}
