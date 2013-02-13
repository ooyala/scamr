package scamr.mapreduce.lib

import scamr.mapreduce.mapper.SimpleMapper
import org.apache.hadoop.mapreduce.MapContext

abstract class IdentityMapper[K, V](context: MapContext[_, _, _, _]) extends SimpleMapper[K,V, K, V](context) {
  override def map(key: K, value: V) = emit(key, value)
}