package scamr.mapreduce

import org.apache.hadoop.mapreduce.TaskInputOutputContext

trait CounterUpdater {
  type CounterContextType = TaskInputOutputContext[_, _, _, _]
  val context: CounterContextType;  // abstract val, must be implemented by subclass

  protected def updateCounter(group: String, name: String, delta: Long) {
    this.context.getCounter(group, name).increment(delta)
  }

  protected def updateCounter(counterId: Enum[_], delta: Long) {
    this.context.getCounter(counterId).increment(delta)
  }
}