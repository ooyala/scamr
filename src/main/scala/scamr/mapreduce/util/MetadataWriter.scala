package scamr.mapreduce.util

import org.apache.hadoop.fs.{FileSystem, Path}
import com.lambdaworks.jacks.JacksMapper
import scala.collection.mutable
import org.apache.hadoop.mapreduce.{Job, Counter, CounterGroup}
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat
import org.apache.log4j.Logger
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import java.io.OutputStream

/** Writes job metadata as json in the hidden file _JobData in the output directory
  *
  * Job metadata includes jobId, jobName, startTime, finishTime, counters.
  * Additional job metadata can be provided in the Hadoop configuration "scamr.metadata.provided"
  * as a json string.
  */
object MetadataWriter {

  import scala.collection.JavaConversions._

  val logger = Logger.getLogger(this.getClass)

  /** Write metadata for Job job */
  def writeMetadata(job: Job) {
    var outputStreamOption: Option[OutputStream] = None
    try {
      // Create OutputStream to HDFS file
      val outputDir = job.getConfiguration.getStrings(FileOutputFormat.OUTDIR)(0)
      val resultFile = new Path(outputDir, "_JobData")
      val fs = FileSystem.get(job.getConfiguration)
      outputStreamOption = Some(fs.create(resultFile))

      // read provided metadata
      val providedData = try {
        job.getConfiguration.getStrings("scamr.metadata.provided").map({ providedMetadata =>
          JacksMapper.readValue[Map[Any,Any]](providedMetadata)
        }).reduce( _ ++ _ )
      } catch {
        case _: Throwable =>
          Map[Any, Any]()
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
        "provided" -> providedData
      )
      outputStreamOption.get.write(JacksMapper.writeValueAsString(output).getBytes)

    } catch {
      case x: Throwable =>
        logger.warn(s"Failed to write job metadata file to output location.", x)

    } finally {
      // Ensure OutputStream is closed
      outputStreamOption match {
        case Some(outputStream) => outputStream.close()
        case _ =>
      }
    }
  }
}
