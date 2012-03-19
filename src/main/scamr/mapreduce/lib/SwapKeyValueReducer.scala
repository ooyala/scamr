package scamr.mapreduce.lib

import scamr.mapreduce.reducer.SimpleReducer

abstract class SwapKeyValueReducer[K, V](context: SwapKeyValueReducer[K, V]#ContextType)
    extends SimpleReducer[K, V, V, K](context) {
  override def reduce(key: K, values: Iterator[V]) = values.foreach { emit(_, key) }
}