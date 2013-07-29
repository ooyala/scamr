package scamr.mapreduce.mapper

import com.escalatesoft.subcut.inject.{Injectable, BindingModule}
import java.lang.reflect.InvocationTargetException
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.{Mapper, MapContext}
import scamr.mapreduce.{CounterUpdater, KeyValueEmitter}

abstract class SimpleMapper[K1, V1, K2, V2](val context: MapContext[_, _, _, _])
extends KeyValueEmitter[K2, V2] with CounterUpdater {
  override val _context = context.asInstanceOf[MapContext[K1, V1, K2, V2]]

  def map(key: K1, value: V1)

  def cleanup() {}
}

object SimpleMapper {
  val SimpleMapperClassProperty = "scamr.simple.mapper.class"
  val BindingModuleClassProperty = "scamr.mapper.subcut.binding.module.class"

  def getRunnerClass[K1, V1, K2, V2]: Class[_ <: Mapper[K1, V1, K2, V2]] = classOf[Runner[K1, V1, K2, V2]]

  def setSimpleMapperClass[K1, V1, K2, V2](conf: Configuration, clazz: Class[_ <: SimpleMapper[K1, V1, K2, V2]]) {
    conf.setClass(SimpleMapperClassProperty, clazz, classOf[SimpleMapper[K1, V1, K2, V2]])
  }

  def setBindingModuleClass(conf: Configuration, clazz: Class[_ <: BindingModule]) {
    conf.setClass(BindingModuleClassProperty, clazz, classOf[BindingModule])
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

      try {
        // True iff the mapper is using dependency injection w/ SubCut
        if (classOf[Injectable].isAssignableFrom(mapperClass)) {
          val bindingModuleClass = conf.getClass(BindingModuleClassProperty, null, classOf[BindingModule])
          if (bindingModuleClass == null) {
            throw new RuntimeException(
              "Cannot resolve SubCut binding module! Make sure the '%s' property is set!".format(
                BindingModuleClassProperty))
          }
          val bindingModule = try {
            bindingModuleClass.getField("MODULE$").get(bindingModuleClass).asInstanceOf[BindingModule]
          } catch {
            case e: NoSuchFieldException =>
              throw new RuntimeException("Error creating Injectable SimpleMapper instance. " +
                "Make sure that the SubCut binding module " + bindingModuleClass.getName +
                " is a scala 'object', and is not nested inside a class.", e)
          }
          val constructor = try {
            mapperClass.getConstructor(classOf[MapContext[K1, V1, K2, V2]], classOf[BindingModule])
          } catch {
            case e: NoSuchMethodException =>
              throw new RuntimeException("Error creating Injectable SimpleMapper instance. " +
                "Looks like you forgot to specify the BindingModule as an implicit constructor parameter!", e)
          }

          // make this mapper's context and configuration available for injection
          mapper = bindingModule.modifyBindings { module =>
            module.bind[MapContext[_, _, _, _]] toSingle context
            module.bind[Configuration] toSingle context.getConfiguration
            constructor.newInstance(context, module)
          }
        } else {
          val constructor = mapperClass.getConstructor(classOf[MapContext[K1, V1, K2, V2]])
          mapper = constructor.newInstance(context)
        }
      } catch {
        case e: InvocationTargetException =>
          throw new RuntimeException("Error creating SimpleMapper instance: " + e.getMessage, e)
        case e: InstantiationException =>
          throw new RuntimeException("Error creating SimpleMapper instance: " + e.getMessage, e)
        case e: IllegalAccessException =>
          throw new RuntimeException("Error creating SimpleMapper instance: " + e.getMessage, e)
      }
    }

    override def map(key: K1, value: V1, context: Mapper[K1, V1, K2, V2]#Context) {
      mapper.map(key, value)
    }

    override def cleanup(context: Mapper[K1, V1, K2, V2]#Context) {
      mapper.cleanup()
    }
  }
}
