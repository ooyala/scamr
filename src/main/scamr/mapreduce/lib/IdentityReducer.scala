package scamr.mapreduce.lib

import scamr.mapreduce.reducer.SimpleReducer

abstract class IdentityReducer[K, V](context: IdentityReducer[K, V]#ContextType)
    extends SimpleReducer[K, V, K, V](context) {
  override def reduce(key: K, values: Iterator[V]) = values.foreach { emit(key, _) }
}