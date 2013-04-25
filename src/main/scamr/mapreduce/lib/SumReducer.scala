package scamr.mapreduce.lib

import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapreduce.ReduceContext
import scamr.mapreduce.reducer.SimpleReducer


class SumReducer[K](context: ReduceContext[_,_,_,_]) extends SimpleReducer[K, LongWritable, K, LongWritable](context) {
  override def reduce(word: K, counts: Iterator[LongWritable]) =
    emit(word, new LongWritable(counts.foldLeft(0L)((a, b) => a + b.get)))
}