package scamr.mapreduce.lib

import org.apache.hadoop.mapreduce.MapContext
import scamr.mapreduce.mapper.SimpleMapper
import org.apache.hadoop.io.{Text, LongWritable}

abstract class TextInputMapper[K2, V2](context: MapContext[LongWritable, Text, K2, V2])
  extends SimpleMapper[LongWritable, Text, K2, V2](context);
