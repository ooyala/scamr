package scamr.io

import org.apache.hadoop.io._

object WritableConversions {
  // Null type
  implicit def nullWritableToNull(value: NullWritable): Null = null
  implicit def nullToNullWritable(value: Null): NullWritable = NullWritable.get

  // Booleans
  implicit def booleanWritableToBoolean(value: BooleanWritable): Boolean = value.get
  implicit def booleanToBooleanWritable(value: Boolean): BooleanWritable = new BooleanWritable(value)

  // Fixed-length integer types
  implicit def byteWritableToByte(value: ByteWritable): Byte = value.get
  implicit def byteToByteWritable(value: Byte): ByteWritable = new ByteWritable(value)

  implicit def intWritableToInt(value: IntWritable): Int = value.get
  implicit def intToIntWritable(value: Int): IntWritable = new IntWritable(value)

  implicit def longWritableToLong(value: LongWritable): Long = value.get
  implicit def longToLongWritable(value: Long): LongWritable = new LongWritable(value)

  // Variable-length integer types
  implicit def vIntWritableToInt(value: VIntWritable): Int = value.get
  implicit def intToVIntWritable(value: Int): VIntWritable = new VIntWritable(value)

  implicit def vLongWritableToLong(value: VLongWritable): Long = value.get
  implicit def longToVLongWritable(value: Long): VLongWritable = new VLongWritable(value)

  // Floating-point types
  implicit def floatWritableToFloat(value: FloatWritable): Float = value.get
  implicit def floatToFloatWritable(value: Float): FloatWritable = new FloatWritable(value)

  implicit def doubleWritableToDouble(value: DoubleWritable): Double = value.get
  implicit def doubleToDoubleWritable(value: Double): DoubleWritable = new DoubleWritable(value)

  // Strings and string-like things
  implicit def textToString(value: Text): String = value.toString
  implicit def stringToText(value: String): Text = new Text(value)

  implicit def stringBuilderToText(value: StringBuilder): Text = new Text(value.toString)
  implicit def stringBufferToText(value: StringBuffer): Text = new Text(value.toString)

  // Maps and sorted maps
  implicit def mapToMapWritable[K <: Writable, V <: Writable](value: Map[K,V]): MapWritable = {
    val mapWritable = new MapWritable
    mapWritable.putAll(value)
    mapWritable
  }

  implicit def mapToSortedMapWritable[K <: WritableComparable[K], V <: Writable](value: Map[K,V]): SortedMapWritable = {
    val sortedMapWritable = new SortedMapWritable
    sortedMapWritable.putAll(value)
    sortedMapWritable
  }

  // Typed ArrayWritables
  implicit def booleanArrayToBooleanArrayWritable(value: Array[Boolean]): BooleanArrayWritable =
    new BooleanArrayWritable(value.map(new BooleanWritable(_)))
  implicit def booleanArrayWritableToBooleanArray(value: BooleanArrayWritable): Array[Boolean] =
    value.get.asInstanceOf[Array[BooleanWritable]].map(_.get)

  implicit def floatArrayToFloatArrayWritable(value: Array[Float]): FloatArrayWritable =
    new FloatArrayWritable(value.map(new FloatWritable(_)))
  implicit def floatArrayWritableToFloatArray(value: FloatArrayWritable): Array[Float] =
    value.get.asInstanceOf[Array[FloatWritable]].map(_.get)

  implicit def doubleArrayToDoubleArrayWritable(value: Array[Double]): DoubleArrayWritable =
    new DoubleArrayWritable(value.map(new DoubleWritable(_)))
  implicit def doubleArrayWritableToDoubleArray(value: DoubleArrayWritable): Array[Double] =
    value.get.asInstanceOf[Array[DoubleWritable]].map(_.get)

  implicit def byteArrayToBytesWritable(value: Array[Byte]): BytesWritable = new BytesWritable(value)
  // Make a copy of the contents, because a BytesWritable is mutable. All other writables wrap immutable
  // objects, so this is highly desirable for consistency.
  implicit def bytesWritableToByteArray(value: BytesWritable): Array[Byte] =
    if (value.getLength == 0) {
      new Array[Byte](0)
    } else {
      val bytes = new Array[Byte](value.getLength)
      Array.copy(value.getBytes, 0, bytes, 0, value.getLength)
      bytes
    }

  implicit def intArrayToIntArrayWritable(value: Array[Int]): IntArrayWritable =
    new IntArrayWritable(value.map(new IntWritable(_)))
  implicit def intArrayWritableToIntArray(value: IntArrayWritable): Array[Int] =
    value.get.asInstanceOf[Array[IntWritable]].map(_.get)

  implicit def longArrayToLongArrayWritable(value: Array[Long]): LongArrayWritable =
    new LongArrayWritable(value.map(new LongWritable(_)))
  implicit def longArrayWritableToLongArray(value: LongArrayWritable): Array[Long] =
    value.get.asInstanceOf[Array[LongWritable]].map(_.get)

  implicit def intArrayToVIntArrayWritable(value: Array[Int]): VIntArrayWritable =
    new VIntArrayWritable(value.map(new VIntWritable(_)))
  implicit def vIntArrayWritableToIntArray(value: VIntArrayWritable): Array[Int] =
    value.get.asInstanceOf[Array[VIntWritable]].map(_.get)

  implicit def longArrayToVLongArrayWritable(value: Array[Long]): VLongArrayWritable =
    new VLongArrayWritable(value.map(new VLongWritable(_)))
  implicit def vLongArrayWritableToLongArray(value: VLongArrayWritable): Array[Long] =
    value.get.asInstanceOf[Array[VLongWritable]].map(_.get)

  implicit def stringArrayToTextArrayWritable(value: Array[String]): TextArrayWritable =
    new TextArrayWritable(value.map(new Text(_)))
  implicit def textArrayWritableToStringArray(value: TextArrayWritable): Array[String] =
    value.get.asInstanceOf[Array[Text]].map(_.toString)
}
