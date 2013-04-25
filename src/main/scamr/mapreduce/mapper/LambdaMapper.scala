package scamr.mapreduce.mapper

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Mapper
import scamr.io.SerializableFunction2
import scamr.mapreduce.lambda.LambdaMapContext

// Note: Much of this class was copied from Jonathan Clark's Scadoop project (https://github.com/jhclark/scadoop)
class LambdaMapper[K1, V1, K2, V2] extends Mapper[K1, V1, K2, V2] {
  type FunctionType = Function2[Iterator[(K1, V1)], LambdaMapContext, Iterator[(K2, V2)]]

  override def run(context: Mapper[K1, V1, K2, V2]#Context) {
    super.setup(context.asInstanceOf[this.type#Context])
    val conf = context.getConfiguration
    val lambda: FunctionType = LambdaMapper.getLambdaFunction[K1, V1, K2, V2](conf)

    def next() = context.nextKeyValue() match {
      case true => Some(context.getCurrentKey, context.getCurrentValue)
      case false => None
    }

    val lambdaMapContext = new LambdaMapContext(context)
    val iter = Iterator continually(next) takeWhile { _ != None } map { _.get }
    for ((outKey, outValue) <- lambda(iter, lambdaMapContext)) {
      context.write(outKey, outValue)
    }
    super.cleanup(context.asInstanceOf[this.type#Context])
  }
}

object LambdaMapper {
  val LambdaFunctionProperty = "scamr.lambda.map.function"

  def setLambdaFunction[K1, V1, K2, V2](conf: Configuration,
                                        lambda: LambdaMapper[K1, V1, K2, V2]#FunctionType) {
    conf.set(LambdaFunctionProperty,
      SerializableFunction2.serializeToBase64String(new SerializableFunction2(lambda)))
  }

  def getLambdaFunction[K1, V1, K2, V2](conf: Configuration): LambdaMapper[K1, V1, K2, V2]#FunctionType = {
    SerializableFunction2.deserializeFromBase64String(conf.get(LambdaFunctionProperty))
  }
}