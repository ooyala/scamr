package scamr.mapreduce.combiner

import org.apache.hadoop.mapreduce.{ReduceContext, Reducer}

import org.apache.hadoop.conf.Configuration
import scamr.mapreduce.reducer.SimpleReducer
import java.lang.reflect.InvocationTargetException

abstract class SimpleCombiner[K, V](context: ReduceContext[K, V, K, V]) extends SimpleReducer[K, V, K, V](context);

object SimpleCombiner {
  val SimpleCombinerClassProperty = "scamr.simple.combiner.class"

  def getRunnerClass[K, V] = classOf[Runner[K, V]]

  def setSimpleCombinerClass[K, V](conf: Configuration, clazz: Class[_ <: SimpleReducer[K, V, K, V]]) {
    conf.setClass(SimpleCombinerClassProperty, clazz, classOf[SimpleReducer[K, V, K, V]])
  }

  class Runner[K, V] extends Reducer[K, V, K, V] {
    private var combiner: SimpleReducer[K, V, K, V] = null

    override def setup(context: Reducer[K, V, K, V]#Context) {
      val conf = context.getConfiguration
      val combinerClass = conf.getClass(SimpleCombinerClassProperty, null, classOf[SimpleReducer[K, V, K, V]])
      if (combinerClass == null) {
        throw new RuntimeException(
          "Cannot resolve concrete subclass of SimpleReducer! Make sure the '%s' property is set!".format(
            SimpleCombinerClassProperty))
      }

      val constructor = combinerClass.getConstructor(classOf[ReduceContext[K, V, K, V]])
      combiner = try {
        constructor.newInstance(context)
      } catch {
        case e: InvocationTargetException =>
          throw new RuntimeException("Error creating SimpleCombiner instance: " + e.getMessage, e)
        case e: InstantiationException =>
          throw new RuntimeException("Error creating SimpleCombiner instance: " + e.getMessage, e)
        case e: IllegalAccessException =>
          throw new RuntimeException("Error creating SimpleCombiner instance: " + e.getMessage, e)
      }
    }

    override def reduce(key: K, values: java.lang.Iterable[V], context: Reducer[K, V, K, V]#Context) {
      combiner.reduce(key, scala.collection.JavaConversions.asScalaIterator(values.iterator()))
    }

    override def cleanup(context: Reducer[K, V, K, V]#Context) {
      combiner.cleanup()
    }
  }
}
