package scamr.mapreduce

import org.apache.hadoop.mapreduce.TaskInputOutputContext

trait KeyValueEmitter[KOUT, VOUT] {
  type OutputContextType = TaskInputOutputContext[_, _, KOUT, VOUT]
  val _context: OutputContextType  // abstract val, must be implemented by subclass

  protected def emit(key: KOUT, value: VOUT): Unit = _context.write(key, value)
  protected def emit(key: KOUT, values: Iterable[VOUT]): Unit = values.foreach { _context.write(key, _) }
}