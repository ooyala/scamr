package scamr.mapreduce.lambda

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Reducer
import scamr.io.SerializableFunction2

// Note: Much of this class was copied from Jonathan Clark's Scadoop project (https://github.com/jhclark/scadoop)
abstract class BaseLambdaReducer[K1, V1, K2, V2] extends Reducer[K1, V1, K2, V2] {

  import scala.collection.JavaConversions

  type FunctionType = Function2[Iterator[(K1, Iterator[V1])], LambdaReduceContext, Iterator[(K2, V2)]]

  val functionPropertyName: String

  override def run(context: Reducer[K1, V1, K2, V2]#Context) {
    super.setup(context.asInstanceOf[this.type#Context])
    val conf = context.getConfiguration
    val lambda: FunctionType = BaseLambdaReducer.getLambdaFunction[K1, V1, K2, V2](conf, functionPropertyName)

    def next() = context.nextKeyValue() match {
      case true => Some(context.getCurrentKey, JavaConversions.asScalaIterator(context.getValues.iterator))
      case false => None
    }

    val lambdaReduceContext = new LambdaReduceContext(context)
    val iter = Iterator continually(next) takeWhile { _ != None } map { _.get }
    for ((outKey, outValue) <- lambda(iter, lambdaReduceContext)) {
      context.write(outKey, outValue)
    }
    super.cleanup(context.asInstanceOf[this.type#Context])
  }
}

object BaseLambdaReducer {
  val CombineFunctionProperty = "scamr.lambda.combine.function"
  val ReduceFunctionProperty = "scamr.lambda.reduce.function"

  def setLambdaReduceFunction[K1, V1, K2, V2](conf: Configuration,
                                              lambda: BaseLambdaReducer[K1, V1, K2, V2]#FunctionType) {
    setLambdaFunction(conf, ReduceFunctionProperty, lambda)
  }

  def setLambdaCombineFunction[K2, V2](conf: Configuration,
                                       lambda: BaseLambdaReducer[K2, V2, K2, V2]#FunctionType) {
    setLambdaFunction(conf, CombineFunctionProperty, lambda)
  }

  private def setLambdaFunction[K1, V1, K2, V2](conf: Configuration,
                                                functionPropertyName: String,
                                                lambda: BaseLambdaReducer[K1, V1, K2, V2]#FunctionType) {
    conf.set(functionPropertyName,
      SerializableFunction2.serializeToBase64String(new SerializableFunction2(lambda)))
  }

  def getLambdaFunction[K1, V1, K2, V2](conf: Configuration, functionPropertyName: String):
      BaseLambdaReducer[K1, V1, K2, V2]#FunctionType = {
    SerializableFunction2.deserializeFromBase64String(conf.get(functionPropertyName))
  }
}