package scamr.io

import org.apache.hadoop.io.{Writable, LongWritable, ArrayWritable}

class LongArrayWritable(values: Array[LongWritable]) extends ArrayWritable(classOf[LongWritable]) {
  if (values != null) this.set(values.asInstanceOf[Array[Writable]])
  def this() = this(null)
}
