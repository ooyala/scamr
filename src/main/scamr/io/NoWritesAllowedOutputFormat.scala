package scamr.io

import org.apache.hadoop.mapreduce._
import java.io.IOException

/**
 * An OutputFormat that throws an IOException on all write attempts. Use this for jobs that use MultipleOutputs
 * exclusively, and want to guarantee that the code doesn't accidentally write to a non-functioning OutputFormat,
 * such as NullOutputFormat
 * @tparam K - the key type
 * @tparam V - the value type
 */
class NoWritesAllowedOutputFormat[K,V] extends OutputFormat[K,V] {
  override def getRecordWriter(context: TaskAttemptContext): RecordWriter[K, V] =
    new RecordWriter[K,V]() {
      def write(key: K, value: V) {
        throw new IOException("No writes allowed to this OutputFormat. Did you mean to use MultipleOutputs.write()?")
      }
      def close(context: TaskAttemptContext) {}
    }

  override def checkOutputSpecs(context: JobContext) {}

  override def getOutputCommitter(context: TaskAttemptContext): OutputCommitter = new OutputCommitter {
    override def setupJob(jobContext: JobContext) {}
    override def needsTaskCommit(taskContext: TaskAttemptContext): Boolean = false
    override def setupTask(taskContext: TaskAttemptContext) {}
    override def commitTask(taskContext: TaskAttemptContext) {}
    override def abortTask(taskContext: TaskAttemptContext) {}
  }
}
