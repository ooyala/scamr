package scamr.mapreduce.util

import org.apache.hadoop.fs.{FileSystem, Path}
import com.lambdaworks.jacks.JacksMapper
import scala.collection.mutable
import org.apache.hadoop.mapreduce.{Job, Counter, CounterGroup}
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat
import org.apache.log4j.Logger

object MetadataWriter {
  val logger = Logger.getLogger(this.getClass)

  def writeMetadata(job: Job) {
    try {

      val outputDir = job.getConfiguration.getStrings("mapreduce.output.fileoutputformat.outputdir")(0)
      val resultFile = new Path(outputDir, "_JobData")
      val fs = FileSystem.get(job.getConfiguration)
      val os = fs.create(resultFile)
      val providedData = try {
        JacksMapper.readValue[Map[Any, Any]](job.getConfiguration.getStrings("scamr.metadata.provided")(0))
      } catch {
        case _: Throwable =>
          Map[Any, Any]()
      }

      val counters = job.getCounters
      val counterMap = mutable.Map[String, Long]()
      for (group: CounterGroup <- counters) {
        for (counter: Counter <- group) {
          counterMap ++= Map(counter.getName -> counter.getValue)
        }
      }

      val output = Map(
        "jobId" -> job.getJobID.toString,
        "jobName" -> job.getJobName,
        "finishTime" -> new DateTime(job.getFinishTime).toString(ISODateTimeFormat.dateHourMinuteSecond()),
        "counters" -> counterMap,
        "provided" -> providedData
      )
      os.write(JacksMapper.writeValueAsString(output).getBytes)
      os.close()

    } catch {
      case x: Throwable =>
        logger.warn(s"Failed to write job metadata file to output location.", x)
    }
  }
}
