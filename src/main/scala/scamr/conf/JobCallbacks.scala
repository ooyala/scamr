package scamr.conf

import org.apache.hadoop.mapreduce.Job

/* Callbacks that will be called when a job succeeds / fails / throws an exception.
 * Chain them to the job the same way that you would chain ConfModifiers or JobModifiers.
 */

/** On-complete callback - always gets called regardless of job result. The first argument to the
  * callback is the hadoop Job. The second is a Left(exception) if one was thrown, a Right(true) if the
  * job succeeded, and a Right(false) if the job failed without throwing an exception.
  * @param callback the callback to call
  */
class OnJobCompletion(private val callback: (Job, Either[Throwable, Boolean]) => Unit)
extends Function2[Job, Either[Throwable, Boolean], Unit] {
  override def apply(job: Job, resultOrError: Either[Throwable, Boolean]): Unit = callback(job, resultOrError)
}

object OnJobCompletion {
  def apply(callback: (Job, Either[Throwable, Boolean]) => Unit): OnJobCompletion = new OnJobCompletion(callback)
}

/** On-success callback - only called if the job succeeds. The only argument to the callback is the hadoop Job.
  * @param successCallback the callback to call
  */
class OnJobSuccess(private val successCallback: Job => Unit)
extends OnJobCompletion((job, resultOrError) => if (resultOrError == Right(true)) successCallback(job))

object OnJobSuccess {
  def apply(successCallback: Job => Unit): OnJobSuccess = new OnJobSuccess(successCallback)
}

/** On-failure callback - only called if the job fails gracefully (returns false) or throws an exception.
  * The only argument to the callback is the hadoop Job. If an exception is thrown, the exception details
  * will not be available to the callback.
  * @param failureCallback the callback to call
  */
class OnJobFailure(private val failureCallback: Job => Unit)
extends OnJobCompletion((job, resultOrError) =>
  if (resultOrError == Right(false) || resultOrError.isLeft) failureCallback(job))

object OnJobFailure {
  def apply(failureCallback: Job => Unit): OnJobFailure = new OnJobFailure(failureCallback)
}

/** On-exception callback - only called if the job throws an exception. The first argument to the callback is
  * the hadoop Job. The second argument is the exception that was thrown.
  * @param errorCallback the callback to call
  */
class OnJobException(private val errorCallback: (Job, Throwable) => Unit)
extends OnJobCompletion((job, resultOrError) => if (resultOrError.isLeft) errorCallback(job, resultOrError.left.get))

object OnJobException {
  def apply(errorCallback: (Job, Throwable) => Unit): OnJobException = new OnJobException(errorCallback)
}