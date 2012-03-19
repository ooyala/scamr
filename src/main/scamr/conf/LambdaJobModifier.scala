package scamr.conf

import org.apache.hadoop.mapreduce.Job

/**
 * A JobModifier that takes a lambda function and calls it from apply(). Makes it easy to specify anonymous
 * JobModifiers for the ScaMR framework without needing to create subclasses.
 */
class LambdaJobModifier(private val lambda: (Job) => Unit) extends JobModifier {
  override def apply(job: Job) = lambda(job)
}

object LambdaJobModifier {
  def apply(lambda: (Job) => Unit) = new LambdaJobModifier(lambda)
}