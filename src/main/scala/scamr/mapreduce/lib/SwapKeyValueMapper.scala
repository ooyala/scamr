package scamr.mapreduce.lib

import org.apache.hadoop.mapreduce.MapContext
import scamr.mapreduce.mapper.SimpleMapper

abstract class SwapKeyValueMapper[K, V](context: MapContext[_, _, _, _]) extends SimpleMapper[K, V, V, K](context) {
  override def map(key: K, value: V): Unit = emit(value, key)
}
