package scamr.io

import org.apache.hadoop.io.{Writable, IntWritable, ArrayWritable}

class IntArrayWritable(values: Array[IntWritable]) extends ArrayWritable(classOf[IntWritable]) {
  if (values != null) this.set(values.asInstanceOf[Array[Writable]])
  def this() = this(null)
}
