package scamr.mapreduce.lib

import scamr.mapreduce.mapper.SimpleMapper

abstract class SwapKeyValueMapper[K, V](context: SwapKeyValueMapper[K, V]#ContextType)
    extends SimpleMapper[K, V, V, K](context) {
  override def map(key: K, value: V) = emit(value, key)
}