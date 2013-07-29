package scamr.mapreduce.combiner

import org.apache.hadoop.conf.Configuration
import scamr.mapreduce.lambda.BaseLambdaReducer

class LambdaCombiner[K2, V2] extends BaseLambdaReducer[K2, V2, K2, V2] {
  override val functionPropertyName = BaseLambdaReducer.CombineFunctionProperty
}

object LambdaCombiner {
  def setLambdaFunction[K2, V2](conf: Configuration, lambda: BaseLambdaReducer[K2, V2, K2, V2]#FunctionType) {
    BaseLambdaReducer.setLambdaCombineFunction(conf, lambda)
  }
}