package scamr.conf

import org.apache.hadoop.conf.Configuration

/**
 * A ConfModifier that takes a lambda function and calls it from apply(). Makes it easy to specify anonymous
 * ConfModifiers for the ScaMR framework without needing to create subclasses.
 */
class LambdaConfModifier(private val lambda: (Configuration) => Unit) extends ConfModifier {
  override def apply(conf: Configuration) = lambda(conf)
}

object LambdaConfModifier {
  def apply(lambda: (Configuration) => Unit) = new LambdaConfModifier(lambda)
}