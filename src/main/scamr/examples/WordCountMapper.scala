package scamr.examples

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.MapContext
import scamr.mapreduce.lib.TextInputMapper

class WordCountMapper(context: MapContext[_, _, _, _]) extends TextInputMapper[Text, LongWritable](context) {
  private val One = new LongWritable(1L)

  override def map(offset: LongWritable, line: Text) =
    line.toString.split("\\s+").foreach { word => if (!word.isEmpty) emit(new Text(word), One) }
}
