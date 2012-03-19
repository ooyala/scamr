package scamr.mapreduce.mapper

import org.apache.hadoop.mapreduce.{Mapper, MapContext}
import org.apache.hadoop.conf.Configuration
import scamr.mapreduce.{CounterUpdater, KeyValueEmitter}

abstract class SimpleMapper[K1, V1, K2, V2](override val context: MapContext[K1, V1, K2, V2])
    extends KeyValueEmitter[K2, V2] with CounterUpdater {
  type ContextType = MapContext[K1, V1, K2, V2]

  def map(key: K1, value: V1)

  def cleanup() {}
}

object SimpleMapper {
  val SimpleMapperClassProperty = "scamr.simple.mapper.class"

  def getRunnerClass[K1, V1, K2, V2] = classOf[Runner[K1, V1, K2, V2]]

  def setSimpleMapperClass[K1, V1, K2, V2](conf: Configuration, clazz: Class[_ <: SimpleMapper[K1, V1, K2, V2]]) {
    conf.setClass(SimpleMapperClassProperty, clazz, classOf[SimpleMapper[K1, V1, K2, V2]])
  }

  class Runner[K1, V1, K2, V2] extends Mapper[K1, V1, K2, V2] {
    private var mapper: SimpleMapper[K1, V1, K2, V2] = null

    override def setup(context: Mapper[K1, V1, K2, V2]#Context) {
      val conf = context.getConfiguration
      val mapperClass = conf.getClass(SimpleMapperClassProperty, null, classOf[SimpleMapper[K1, V1, K2, V2]])
      if (mapperClass == null) {
        throw new RuntimeException(
          "Cannot resolve concrete subclass of SimpleMapper! Make sure the '%s' property is set!".format(
            SimpleMapperClassProperty))
      }
      val constructor = mapperClass.getConstructor(classOf[MapContext[K1, V1, K2, V2]])
      mapper = constructor.newInstance(context)
    }

    override def map(key: K1, value: V1, context: Mapper[K1, V1, K2, V2]#Context) {
      mapper.map(key, value)
    }

    override def cleanup(context: Mapper[K1, V1, K2, V2]#Context) {
      mapper.cleanup()
    }
  }

}