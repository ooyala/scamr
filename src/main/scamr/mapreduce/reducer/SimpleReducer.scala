package scamr.mapreduce.reducer

import org.apache.hadoop.mapreduce.{Reducer, ReduceContext}
import org.apache.hadoop.conf.Configuration
import scamr.mapreduce.{CounterUpdater, KeyValueEmitter}

abstract class SimpleReducer[K1, V1, K2, V2](override val context: ReduceContext[K1, V1, K2, V2])
    extends KeyValueEmitter[K2, V2] with CounterUpdater {
  type ContextType = ReduceContext[K1, V1, K2, V2]

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

  def getRunnerClass[K1, V1, K2, V2] = classOf[Runner[K1, V1, K2, V2]]

  def setSimpleReducerClass[K1, V1, K2, V2](conf: Configuration, clazz: Class[_ <: SimpleReducer[K1, V1, K2, V2]]) {
    conf.setClass(SimpleReducerClassProperty, clazz, classOf[SimpleReducer[K1, V1, K2, V2]])
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
      val constructor = reducerClass.getConstructor(classOf[ReduceContext[K1, V1, K2, V2]])
      reducer = constructor.newInstance(context)
    }

    override def reduce(key: K1, values: java.lang.Iterable[V1], context: Reducer[K1, V1, K2, V2]#Context) {
      reducer.reduce(key, scala.collection.JavaConversions.asScalaIterator(values.iterator()))
    }

    override def cleanup(context: Reducer[K1, V1, K2, V2]#Context) {
      reducer.cleanup()
    }
  }

}