package scamr.mapreduce.lib

import scamr.mapreduce.reducer.SimpleReducer
import org.apache.hadoop.mapreduce.ReduceContext

abstract class SwapKeyValueReducer[K, V](context: ReduceContext[_, _, _, _]) extends SimpleReducer[K, V, V, K](context) {
  override def reduce(key: K, values: Iterator[V]) = values.foreach { emit(_, key) }
}