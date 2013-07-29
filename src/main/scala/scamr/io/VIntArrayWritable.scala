package scamr.io

import org.apache.hadoop.io.{Writable, VIntWritable, ArrayWritable}

class VIntArrayWritable(values: Array[VIntWritable]) extends ArrayWritable(classOf[VIntWritable]) {
  if (values != null) this.set(values.asInstanceOf[Array[Writable]])
  def this() = this(null)
}