package scamr.io

import org.apache.hadoop.io.{Writable, BooleanWritable, ArrayWritable}

class BooleanArrayWritable(values: Array[BooleanWritable]) extends ArrayWritable(classOf[BooleanWritable]) {
  if (values != null) this.set(values.asInstanceOf[Array[Writable]])
  def this() = this(null)
}