package scamr.mapreduce.lambda

import org.apache.hadoop.mapreduce.MapContext

class LambdaMapContext(_context: MapContext[_, _, _, _]) extends BaseLambdaContext(_context) {
  def getInputSplit = _context.getInputSplit
}
