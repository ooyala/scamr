package scamr.mapreduce

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.util.ReflectionUtils

class CopyingIterator[T](conf: Configuration, javaIterator: java.util.Iterator[T]) extends Iterator[T] {
  def this(conf: Configuration, javaIterable: java.lang.Iterable[T]) = this(conf, javaIterable.iterator())
  override def hasNext: Boolean = javaIterator.hasNext
  override def next(): T = ReflectionUtils.copy[T](conf, javaIterator.next(), null.asInstanceOf[T])
}
