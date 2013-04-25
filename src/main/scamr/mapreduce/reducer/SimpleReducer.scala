package scamr.mapreduce.reducer

import org.apache.hadoop.mapreduce.{Reducer, ReduceContext}
import org.apache.hadoop.conf.Configuration
import scamr.mapreduce.{CounterUpdater, KeyValueEmitter}
import java.lang.reflect.InvocationTargetException
import com.escalatesoft.subcut.inject.{BindingModule, Injectable}

abstract class SimpleReducer[K1, V1, K2, V2](val context: ReduceContext[_, _, _, _])
    extends KeyValueEmitter[K2, V2] with CounterUpdater {
  override val _context = context.asInstanceOf[ReduceContext[K1, V1, K2, V2]]

  // WARNING: Trying to do anything other than a foreach() loop with values() might not do what you want,
  // because Hadoop likely reuses a single instance of your Writable object and reads successive values
  // into it. For example, calling values.toList() may return a list of entries that are all copies of the
  // last one.
  //
  // TODO(ivmaykov): Maybe implement some magic glue code that will fix issues like this at the cost of
  // efficiency? In general, loading all values into memory may be unsafe in a large job and may run the
  // process OOM if there are too many values for a key, which is why iterating with foreach(), which uses
  // constant space, is recommended.
  //
  // Or maybe we should use a different class that only supports iteration with foreach(), without any
  // of the other operations that Scala's Iterator provides?
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
      reducer.reduce(key, scala.collection.JavaConversions.asScalaIterator(values.iterator()))
    }

    override def cleanup(context: Reducer[K1, V1, K2, V2]#Context) {
      reducer.cleanup()
    }
  }

}