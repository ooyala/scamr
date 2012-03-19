package scamr.examples

import scamr.mapreduce.mapper.SimpleMapper
import org.apache.hadoop.io.{LongWritable, Text}

class WordCountMapper(context: WordCountMapper#ContextType)
    extends SimpleMapper[LongWritable, Text, Text, LongWritable](context) {
  private val One = new LongWritable(1L)

  override def map(offset: LongWritable, line: Text) =
    line.toString.split("\\s+").foreach { word => if (!word.isEmpty) emit(new Text(word), One) }
}
