package scamr.mapreduce.lib

import org.apache.hadoop.mapreduce.ReduceContext
import scamr.mapreduce.reducer.SimpleReducer

abstract class SwapKeyValueReducer[K, V](context: ReduceContext[_, _, _, _])
extends SimpleReducer[K, V, V, K](context) {
  override def reduce(key: K, values: Iterator[V]): Unit = values.foreach { emit(_, key) }
}
