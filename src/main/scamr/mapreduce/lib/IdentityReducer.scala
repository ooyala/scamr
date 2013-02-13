package scamr.mapreduce.lib

import scamr.mapreduce.reducer.SimpleReducer
import org.apache.hadoop.mapreduce.ReduceContext

abstract class IdentityReducer[K, V](context: ReduceContext[_, _, _, _]) extends SimpleReducer[K, V, K, V](context) {
  override def reduce(key: K, values: Iterator[V]) = values.foreach { emit(key, _) }
}