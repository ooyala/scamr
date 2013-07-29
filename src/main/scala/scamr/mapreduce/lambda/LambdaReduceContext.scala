package scamr.mapreduce.lambda

import org.apache.hadoop.mapreduce.ReduceContext

class LambdaReduceContext(_context: ReduceContext[_, _, _, _]) extends BaseLambdaContext(_context)
