package scamr.mapreduce.lambda

import org.apache.hadoop.mapreduce.MapContext

class LambdaMapContext[K1, V1, K2, V2](context: MapContext[K1, V1, K2, V2])
    extends BaseLambdaContext[K1, V1, K2, V2](context) {
  def getInputSplit = context.getInputSplit
}
