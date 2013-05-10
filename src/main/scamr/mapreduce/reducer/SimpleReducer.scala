package scamr.mapreduce.reducer

import com.escalatesoft.subcut.inject.{BindingModule, Injectable}
import java.lang.reflect.InvocationTargetException
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.{Reducer, ReduceContext}
import scamr.mapreduce.{CopyingIterator, CounterUpdater, KeyValueEmitter}

abstract class SimpleReducer[K1, V1, K2, V2](val context: ReduceContext[_, _, _, _])
    extends KeyValueEmitter[K2, V2] with CounterUpdater {
  override val _context = context.asInstanceOf[ReduceContext[K1, V1, K2, V2]]

  def reduce(key: K1, values: Iterator[V1])

  def cleanup() {}
}

object SimpleReducer {
  val SimpleReducerClassProperty = "scamr.simple.reducer.class"
  val BindingModuleClassProperty = "scamr.reducer.subcut.binding.module.class"

  def getRunnerClass[K1, V1, K2, V2] = classOf[Runner[K1, V1, K2, V2]]

  def setSimpleReducerClass[K1, V1, K2, V2](conf: Configuration, clazz: Class[_ <: SimpleReducer[K1, V1, K2, V2]]) {
    conf.setClass(SimpleReducerClassProperty, clazz, classOf[SimpleReducer[K1, V1, K2, V2]])
  }

  def setBindingModuleClass(conf: Configuration, clazz: Class[_ <: BindingModule]) {
    conf.setClass(BindingModuleClassProperty, clazz, classOf[BindingModule])
  }

  class Runner[K1, V1, K2, V2] extends Reducer[K1, V1, K2, V2] {
    private var reducer: SimpleReducer[K1, V1, K2, V2] = null

    override def setup(context: Reducer[K1, V1, K2, V2]#Context) {
      val conf = context.getConfiguration
      val reducerClass = conf.getClass(SimpleReducerClassProperty, null, classOf[SimpleReducer[K1, V1, K2, V2]])
      if (reducerClass == null) {
        throw new RuntimeException(
          "Cannot resolve concrete subclass of SimpleReducer! Make sure the '%s' property is set!".format(
            SimpleReducerClassProperty))
      }

      try {
        // True iff the reducer is using dependency injection w/ SubCut
        if (classOf[Injectable].isAssignableFrom(reducerClass)) {
          val bindingModuleClass = conf.getClass(BindingModuleClassProperty, null, classOf[BindingModule])
          if (bindingModuleClass == null) {
            throw new RuntimeException(
              "Cannot resolve SubCut binding module! Make sure the '%s' property is set!".format(BindingModuleClassProperty))
          }
          val bindingModule = try {
            bindingModuleClass.getField("MODULE$").get(bindingModuleClass).asInstanceOf[BindingModule]
          } catch {
            case e: NoSuchFieldException =>
              throw new RuntimeException("Error creating Injectable SimpleReducer instance. " +
                "Make sure that the SubCut binding module " + bindingModuleClass.getName +
                " is a scala 'object', and is not nested inside a class.", e)
          }
          val constructor = try {
              reducerClass.getConstructor(classOf[ReduceContext[K1, V1, K2, V2]], classOf[BindingModule])
          } catch {
            case e: NoSuchMethodException =>
              throw new RuntimeException("Error creating Injectable SimpleReducer instance. " +
                "Looks like you forgot to specify the BindingModule as an implicit constructor parameter!", e)
          }

          // make this reducer's context and configuration available for injection
          reducer = bindingModule.modifyBindings { module =>
            module.bind [ReduceContext[_, _, _, _]] toSingle context
            module.bind [Configuration] toSingle context.getConfiguration
            constructor.newInstance(context, module)
          }
        } else {
          val constructor = reducerClass.getConstructor(classOf[ReduceContext[K1, V1, K2, V2]])
          reducer = constructor.newInstance(context)
        }
      } catch {
        case e: InvocationTargetException =>
          throw new RuntimeException("Error creating SimpleReducer instance: " + e.getMessage, e)
        case e: InstantiationException =>
          throw new RuntimeException("Error creating SimpleReducer instance: " + e.getMessage, e)
        case e: IllegalAccessException =>
          throw new RuntimeException("Error creating SimpleReducer instance: " + e.getMessage, e)
      }
    }

    override def reduce(key: K1, values: java.lang.Iterable[V1], context: Reducer[K1, V1, K2, V2]#Context) {
      reducer.reduce(key, new CopyingIterator(context.getConfiguration, values))
    }

    override def cleanup(context: Reducer[K1, V1, K2, V2]#Context) {
      reducer.cleanup()
    }
  }

}