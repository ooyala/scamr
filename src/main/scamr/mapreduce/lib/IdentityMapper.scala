package scamr.mapreduce.lib

import org.apache.hadoop.mapreduce.MapContext
import scamr.mapreduce.mapper.SimpleMapper

abstract class IdentityMapper[K, V](context: MapContext[_, _, _, _]) extends SimpleMapper[K,V, K, V](context) {
  override def map(key: K, value: V) = emit(key, value)
}