package scamr.mapreduce.lambda

import org.apache.hadoop.mapreduce.TaskInputOutputContext
import scamr.mapreduce.CounterUpdater

// A wrapper around Hadoop's TaskInputOutputContext that only exposes limited functionality.
// Specifically, it doesn't allow calls to any mutating functions except for updateCounter(), setStatus(), and progress().
class BaseLambdaContext(override val _context: TaskInputOutputContext[_, _, _, _]) extends CounterUpdater {
  // Side-effect-free methods
  def getTaskAttemptId = _context.getTaskAttemptID
  def getStatus = _context.getStatus
  def getConfiguration = _context.getConfiguration
  def getJobId = _context.getJobID
  def getJobName = _context.getJobName
  def getNumReduceTasks = _context.getNumReduceTasks
  def getWorkingDirectory = _context.getWorkingDirectory

  // Side-effect-full methods
  def setStatus(status: String) {
    _context.setStatus(status)
  }

  def progress() = _context.progress()
}