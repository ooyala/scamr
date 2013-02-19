package scamr.io

import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTimeZone, DateTime}
import org.apache.hadoop.fs.Path

object InputOutputUtils {
  lazy val random = new scala.util.Random()

  def randomWorkingDir(prefix: String, jobName: String = ""): String = {
    val now = DateTimeFormat.forPattern("YYYY-MM-dd-HH-mm-ss").print(new DateTime(System.currentTimeMillis,
      DateTimeZone.UTC))
    val randomLong = random.nextLong.abs.toString
    // Extract a sanitized job name component from the full job name by replacing all whitespace with underscores
    // and all non-word characters (a-zA-Z_0-9) with empty strings.
    val sanitizedName = jobName.replaceAll("\\s+", "_").replaceAll("\\W+", "")
    new Path(prefix, (System.getenv("USER") + "-" + now + "-" + sanitizedName + "-" + randomLong).toLowerCase).toString
  }
}
