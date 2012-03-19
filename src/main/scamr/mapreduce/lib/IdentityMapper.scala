package scamr.mapreduce.lib

import scamr.mapreduce.mapper.SimpleMapper

abstract class IdentityMapper[K, V](context: IdentityMapper[K, V]#ContextType)
    extends SimpleMapper[K,V, K, V](context) {
  override def map(key: K, value: V) = emit(key, value)
}