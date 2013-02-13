package scamr.mapreduce.lib

import scamr.mapreduce.mapper.SimpleMapper
import org.apache.hadoop.mapreduce.MapContext

abstract class SwapKeyValueMapper[K, V](context: MapContext[_, _, _, _]) extends SimpleMapper[K, V, V, K](context) {
  override def map(key: K, value: V) = emit(value, key)
}