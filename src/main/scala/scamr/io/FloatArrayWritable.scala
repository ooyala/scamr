package scamr.io

import org.apache.hadoop.io.{Writable, FloatWritable, ArrayWritable}

class FloatArrayWritable(values: Array[FloatWritable]) extends ArrayWritable(classOf[FloatWritable]) {
  if (values != null) this.set(values.asInstanceOf[Array[Writable]])
  def this() = this(null)
}
