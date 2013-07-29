package scamr.mapreduce.lambda

import org.apache.hadoop.mapreduce.{JobID, TaskAttemptID, TaskInputOutputContext}
import scamr.mapreduce.CounterUpdater
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

// A wrapper around Hadoop's TaskInputOutputContext that only exposes limited functionality.
// Specifically, it doesn't allow calls to any mutating functions except for updateCounter(), setStatus(),
// and progress().
class BaseLambdaContext(override val _context: TaskInputOutputContext[_, _, _, _]) extends CounterUpdater {
  // Side-effect-free methods
  def getTaskAttemptId: TaskAttemptID = _context.getTaskAttemptID
  def getStatus: String = _context.getStatus
  def getConfiguration: Configuration = _context.getConfiguration
  def getJobId: JobID = _context.getJobID
  def getJobName: String = _context.getJobName
  def getNumReduceTasks: Int = _context.getNumReduceTasks
  def getWorkingDirectory: Path = _context.getWorkingDirectory

  // Side-effect-full methods
  def setStatus(status: String) {
    _context.setStatus(status)
  }

  def progress() { _context.progress() }
}
