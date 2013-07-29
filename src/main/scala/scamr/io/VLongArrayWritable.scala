package scamr.io

import org.apache.hadoop.io.{Writable, VLongWritable, ArrayWritable}

class VLongArrayWritable(values: Array[VLongWritable]) extends ArrayWritable(classOf[VLongWritable]) {
  if (values != null) this.set(values.asInstanceOf[Array[Writable]])
  def this() = this(null)
}
