package scamr.mapreduce.combiner

import com.escalatesoft.subcut.inject.{Injectable, BindingModule}
import java.lang.reflect.InvocationTargetException
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.{ReduceContext, Reducer}
import scamr.mapreduce.CopyingIterator
import scamr.mapreduce.reducer.SimpleReducer


abstract class SimpleCombiner[K, V](context: ReduceContext[_, _, _, _]) extends SimpleReducer[K, V, K, V](context)

object SimpleCombiner {
  val SimpleCombinerClassProperty = "scamr.simple.combiner.class"
  val BindingModuleClassProperty = "scamr.combiner.subcut.binding.module.class"

  def getRunnerClass[K, V]: Class[_ <: Reducer[K, V, K, V]] = classOf[Runner[K, V]]

  def setSimpleCombinerClass[K, V](conf: Configuration, clazz: Class[_ <: SimpleReducer[K, V, K, V]]) {
    conf.setClass(SimpleCombinerClassProperty, clazz, classOf[SimpleReducer[K, V, K, V]])
  }

  def setBindingModuleClass(conf: Configuration, clazz: Class[_ <: BindingModule]) {
    conf.setClass(BindingModuleClassProperty, clazz, classOf[BindingModule])
  }

  class Runner[K, V] extends Reducer[K, V, K, V] {
    private var combiner: SimpleReducer[K, V, K, V] = null

    override def setup(context: Reducer[K, V, K, V]#Context) {
      val conf = context.getConfiguration
      val combinerClass = conf.getClass(SimpleCombinerClassProperty, null, classOf[SimpleReducer[K, V, K, V]])
      if (combinerClass == null) {
        throw new RuntimeException(
          s"Cannot resolve concrete subclass of SimpleReducer! Make sure the '$SimpleCombinerClassProperty' property is set!")
      }

      try {
        if (classOf[Injectable].isAssignableFrom(combinerClass)) {
          // True iff the combiner is using dependency injection w/ SubCut
          combiner = createInjectable(context, combinerClass)
        } else {
          val constructor = combinerClass.getConstructor(classOf[ReduceContext[K, V, K, V]])
          combiner = constructor.newInstance(context)
        }
      } catch {
        case e: InvocationTargetException =>
          throw new RuntimeException(s"Error creating SimpleCombiner instance: ${e.getMessage}", e)
        case e: InstantiationException =>
          throw new RuntimeException(s"Error creating SimpleCombiner instance: ${e.getMessage}", e)
        case e: IllegalAccessException =>
          throw new RuntimeException(s"Error creating SimpleCombiner instance: ${e.getMessage}", e)
      }
    }

    private def createInjectable(context: Reducer[K, V, K, V]#Context,
                                 clazz: Class[_ <: SimpleReducer[K, V, K, V]]): SimpleReducer[K, V, K, V] = {
      require(classOf[Injectable].isAssignableFrom(clazz), s"Must extend the Injectable trait: ${clazz.getName}")

      val conf = context.getConfiguration
      val bindingModuleClass = conf.getClass(BindingModuleClassProperty, null, classOf[BindingModule])
      if (bindingModuleClass == null) {
        throw new RuntimeException(
          s"Cannot resolve SubCut binding module! Make sure the '$BindingModuleClassProperty' property is set!")
      }
      val bindingModule = try {
        bindingModuleClass.getField("MODULE$").get(bindingModuleClass).asInstanceOf[BindingModule]
      } catch {
        case e: NoSuchFieldException =>
          throw new RuntimeException("Error creating Injectable SimpleCombiner instance. " +
            s"Make sure that the SubCut binding module ${bindingModuleClass.getName} " +
            "is a scala 'object', and is not nested inside a class.", e)
      }
      val constructor = try {
        clazz.getConstructor(classOf[ReduceContext[K, V, K, V]], classOf[BindingModule])
      } catch {
        case e: NoSuchMethodException =>
          throw new RuntimeException("Error creating Injectable SimpleCombiner instance. " +
            "Looks like you forgot to specify the BindingModule as an implicit constructor parameter!", e)
      }

      // make this combiner's context and configuration available for injection
      val result = bindingModule.modifyBindings { module =>
        module.bind[ReduceContext[_, _, _, _]] toSingle context
        module.bind[Configuration] toSingle context.getConfiguration
        constructor.newInstance(context, module)
      }
      result
    }

    override def reduce(key: K, values: java.lang.Iterable[V], context: Reducer[K, V, K, V]#Context) {
      combiner.reduce(key, new CopyingIterator(context.getConfiguration, values))
    }

    override def cleanup(context: Reducer[K, V, K, V]#Context) {
      combiner.cleanup()
    }
  }
}
