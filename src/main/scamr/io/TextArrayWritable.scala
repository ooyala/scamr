package scamr.io

import org.apache.hadoop.io.{Writable, Text, ArrayWritable}

class TextArrayWritable(values: Array[Text]) extends ArrayWritable(classOf[Text]) {
  if (values != null) this.set(values.asInstanceOf[Array[Writable]])
  def this() = this(null)
}