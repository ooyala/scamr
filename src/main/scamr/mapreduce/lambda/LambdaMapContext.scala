package scamr.mapreduce.lambda

import org.apache.hadoop.mapreduce.MapContext

class LambdaMapContext(context: MapContext[_, _, _, _]) extends BaseLambdaContext(context) {
  def getInputSplit = context.getInputSplit
}
