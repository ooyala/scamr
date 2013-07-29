package scamr.io

import org.apache.hadoop.io.{Writable, DoubleWritable, ArrayWritable}

class DoubleArrayWritable(values: Array[DoubleWritable]) extends ArrayWritable(classOf[DoubleWritable]) {
  if (values != null) this.set(values.asInstanceOf[Array[Writable]])
  def this() = this(null)
}
