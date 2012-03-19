package scamr.mapreduce.lambda

import org.apache.hadoop.mapreduce.TaskInputOutputContext

// A wrapper around Hadoop's TaskInputOutputContext that only exposes limited functionality.
// Specifically, it doesn't allow calls to any mutating functions except for updateCounter(), setStatus(), and progress().
class BaseLambdaContext[K1, V1, K2, V2](context: TaskInputOutputContext[K1, V1, K2, V2]) {
  // Side-effect-free methods
  def getTaskAttemptId = context.getTaskAttemptID
  def getStatus = context.getStatus
  def getConfiguration = context.getConfiguration
  def getJobId = context.getJobID
  def getJobName = context.getJobName
  def getNumReduceTasks = context.getNumReduceTasks
  def getWorkingDirectory = context.getWorkingDirectory

  // Side-effect-full methods
  def updateCounter(group: String, name: String, delta: Long) {
    context.getCounter(group, name).increment(delta)
  }

  def setStatus(status: String) {
    context.setStatus(status)
  }

  def progress() = context.progress()
}